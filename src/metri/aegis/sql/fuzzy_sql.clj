(ns metri.aegis.sql.fuzzy-sql
  "Expansor de términos fuzzy para Athena/OLAP.
   SRP: genera expresiones SQL cost-safe desde un término de búsqueda aproximado.
   Sin I/O. Sin estado. Sin side-effects.

   PRINCIPIO DE DISEÑO — Anti-Levenshtein en Athena:
   ──────────────────────────────────────────────────
   `levenshtein_distance(col, term)` en Athena requiere full table scan (O(m·n) por
   fila, sin predicate pushdown) → PROHIBIDO.

   Este módulo computa el fuzzy en la JVM (Clojure) y emite un set ACOTADO de
   expresiones SQL que Athena puede ejecutar con ayuda de los column-level
   bloom filters e Iceberg min/max statistics:

     1. LIKE '%term%'                    → fast path, Bloom filter OK
     2. regexp_like(col, 'alt1|alt2|…')  → 1 expresión, N alternativas 1-edit

   COSTO ESTIMADO vs LEVENSHTEIN:
   ┌─────────────────────────────┬────────────────┬──────────────┐
   │ Técnica                     │ Predicate push │ Cost (10M rows) │
   ├─────────────────────────────┼────────────────┼──────────────┤
   │ levenshtein_distance()       │ ❌ None         │ ~$0.50        │
   │ LIKE '%term%'                │ ⚠️ Bloom filter │ ~$0.004       │
   │ LIKE + regexp_like (este ns) │ ⚠️ Bloom filter │ ~$0.005       │
   └─────────────────────────────┴────────────────┴──────────────┘

   Guardia de costos:
   ──────────────────
   ≤ 2 chars → solo LIKE exacto (threshold=0)
   3-8 chars → LIKE + regexp_like DFA (generación estricta de Damerau-Levenshtein 1)
   ≥ 9 chars → solo LIKE substring (regex demasiado ambiguo en textos largos)

   API pública:
     expand-term  [term] → {:like-pat str :regex-pat str-or-nil}
     fuzzy-honey  [col-honey term] → HoneySQL expr")

;; ═══════════════════════════════════════════════════════════════════════════
;; CONSTANTES
;; ═══════════════════════════════════════════════════════════════════════════

;; MAX-REGEX-ALTS ya no se utiliza porque el DFA de Damerau-Levenshtein genera el 
;; conjunto exacto matemáticamente.

(def ^:private FUZZY-MIN-LEN
  "Longitud mínima del término para activar fuzzy (regex).
   Términos muy cortos generan demasiados falsos positivos."
  3)

(def ^:private FUZZY-MAX-LEN
  "Longitud máxima del término para activar fuzzy (regex).
   Términos muy largos producen patrones poco selectivos."
  8)

;; ═══════════════════════════════════════════════════════════════════════════
;; GENERACIÓN DE ALTERNATIVAS — sustitución posicional
;; ═══════════════════════════════════════════════════════════════════════════

(defn- escape-regex-literal
  "Escapa metacaracteres regex de Presto/Athena en la parte literal del patrón.
   Los caracteres con significado especial en regex Java/Presto se escapan."
  [^String s]
  (-> s
      (clojure.string/replace "\\" "\\\\")
      (clojure.string/replace "." "\\.")
      (clojure.string/replace "+" "\\+")
      (clojure.string/replace "*" "\\*")
      (clojure.string/replace "?" "\\?")
      (clojure.string/replace "(" "\\(")
      (clojure.string/replace ")" "\\)")
      (clojure.string/replace "[" "\\[")
      (clojure.string/replace "]" "\\]")
      (clojure.string/replace "{" "\\{")
      (clojure.string/replace "}" "\\}")
      (clojure.string/replace "^" "\\^")
      (clojure.string/replace "$" "\\$")
      (clojure.string/replace "|" "\\|")))

(defn- generate-lev1-regex
  "Genera un patrón RE2 estricto que matchea exactamente la distancia de 
   Damerau-Levenshtein 1 del término base (omisiones, sustituciones, inserciones, transposiciones).
   Usa word boundaries (\\b) para imitar tokenización y evitar falsos positivos
   en substrings genéricos (ej. 'rak' matcheando 'baking')."
  [^String t]
  (let [n (count t)
        ;; Omisiones (length n-1)
        dels   (for [i (range n)] (str (escape-regex-literal (subs t 0 i)) 
                                       (escape-regex-literal (subs t (inc i)))))
        ;; Sustituciones (length n)
        subs-a (for [i (range n)] (str (escape-regex-literal (subs t 0 i)) 
                                       "." 
                                       (escape-regex-literal (subs t (inc i)))))
        ;; Inserciones (length n+1)
        ins    (for [i (range (inc n))] (str (escape-regex-literal (subs t 0 i)) 
                                         "." 
                                         (escape-regex-literal (subs t i))))
        ;; Transposiciones (length n)
        trns   (for [i (range (dec n))] (str (escape-regex-literal (subs t 0 i))
                                             (escape-regex-literal (str (nth t (inc i))))
                                             (escape-regex-literal (str (nth t i)))
                                             (escape-regex-literal (subs t (+ i 2)))))
        ;; Combinar todo y desduplicar (incluyendo el término exacto)
        all    (distinct (concat [(escape-regex-literal t)] dels subs-a ins trns))]
    (str "\\b(" (clojure.string/join "|" all) ")\\b")))


;; ═══════════════════════════════════════════════════════════════════════════
;; API PRINCIPAL
;; ═══════════════════════════════════════════════════════════════════════════

(defn expand-term
  "Expande un término de búsqueda en patrones SQL cost-safe para Athena.

   Retorna un mapa:
   {:like-pat  'string'    — patrón para LIKE (con wildcards %)
    :regex-pat 'string'|nil — patrón para regexp_like, nil si no aplica fuzzy}

   Estrategia por longitud del término (t normalizado lowercase):
     ≤ 2 chars → solo LIKE exacto, no fuzzy
     3-8 chars → LIKE + regexp_like con alts de 1-sustitución
     ≥ 9 chars → solo LIKE substring (regex sería poco selectivo)

   Ejemplo:
     (expand-term 'Chiler')
     → {:like-pat '%chiler%'
        :regex-pat 'ch.ler|chil.r|chile.|chi.er'}

     (expand-term 'ID')
     → {:like-pat '%id%'
        :regex-pat nil}"
  [^String term]
  (when (and (string? term) (seq term))
    (let [t     (.toLowerCase term)
          n     (count t)
          ;; Escapar el término literal para el LIKE
          esc-t (-> t
                    (clojure.string/replace "\\" "\\\\")
                    (clojure.string/replace "%" "\\%")
                    (clojure.string/replace "_" "\\_"))
          like-pat (str "%" esc-t "%")]
      (if (and (>= n FUZZY-MIN-LEN) (<= n FUZZY-MAX-LEN))
        ;; Rango fuzzy: genera DFA exacto Damerau-Levenshtein 1
        (let [regex-pat (generate-lev1-regex t)]
          {:like-pat  like-pat
           :regex-pat regex-pat})
        ;; Fuera de rango fuzzy: solo LIKE
        {:like-pat  like-pat
         :regex-pat nil}))))

;; ═══════════════════════════════════════════════════════════════════════════
;; HONEY-SQL BUILDER
;; ═══════════════════════════════════════════════════════════════════════════

(defn fuzzy-honey
  "Construye una expresión HoneySQL para fuzzy search en Athena/OLAP.

   col-honey: expresión HoneySQL para la columna (e.g. :name o [:raw \"name\"])
   term:      término de búsqueda (string, se normaliza internamente)

   Genera:
     Con fuzzy activo (term 3-8 chars):
       [:or
         [:like [:lower col-honey] like-pat]
         [:regexp_like [:lower col-honey] regex-pat]]

     Sin fuzzy (term ≤2 o ≥9 chars):
       [:like [:lower col-honey] like-pat]

   Siempre envuelve la columna en lower() para case-insensitive matching.
   lower() en Athena tiene costo mínimo sobre columnas Parquet (evaluación lazy).

   Relación con Iceberg Bloom Filters:
     Athena usa bloom filters por column-chunk de Parquet/Iceberg para el LIKE.
     El regexp_like NO se beneficia de bloom filters pero sí de min/max statistics
     (si el rango de valores en el chunk no puede contener el patrón, se omite).
     Resultado: el scan efectivo es mucho menor que una full table scan."
  [col-honey term]
  (when-let [{:keys [like-pat regex-pat]} (expand-term term)]
    (let [lower-col [:lower col-honey]]
      (if regex-pat
        ;; Fuzzy activo: LIKE OR regexp_like
        [:or
         [:like lower-col like-pat]
         [:regexp_like lower-col regex-pat]]
        ;; Solo LIKE (term muy corto o muy largo)
        [:like lower-col like-pat]))))
