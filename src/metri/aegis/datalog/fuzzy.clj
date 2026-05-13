(ns metri.aegis.datalog.fuzzy
  "Biblioteca de Fuzzy Matching para Omnisearch en Datahike/OLTP.
   SRP: matching aproximado de cadenas — sin I/O, sin estado.

   Algoritmos implementados (puros, sin dependencias externas):
     1. Levenshtein-Wagner-Fischer    O(m·n) tiempo, O(n) espacio (rolling row).
        Usado para comparar palabras individuales contra el término de búsqueda.
     2. Case-insensitive substring    Fast path: str/includes? sobre lower-case.
        Ejecutado antes que Levenshtein para evitar el costo O(m·n).
     3. Word-level tokenization       Divide el campo en tokens por espacios/guiones.
        Permite que 'Chiller A-01' matchee 'chiller' o 'Chiler' sin contabilizar
        los demás tokens en la distancia.

   Threshold adaptativo:
     ≤ 2 chars  → 0 (sin fuzzy — evita flood de falsos positivos en IDs/siglas)
     3-8 chars  → 1 (1 error tolerado: 'Rack'→'Rak', 'Chiller'→'Chiler')
     ≥ 9 chars  → 2 (2 errores: 'work order'→'wrk ordeer')

   API pública:
     fuzzy-match? [value term]  → boolean
     fuzzy-match-fn             → función capturada apta para :in de Datahike")

;; ═══════════════════════════════════════════════════════════════════════════
;; LEVENSHTEIN — Wagner-Fischer O(m·n), rolling-row O(n)
;; ═══════════════════════════════════════════════════════════════════════════

(defn levenshtein-distance
  "Distancia de edición mínima entre dos strings a y b (case-SENSITIVE).
   Llama siempre con strings ya en lowercase para comparación fuzzy.
   Utiliza un único arreglo int (rolling row) para minimizar allocations."
  ^long [^String a ^String b]
  (let [la (count a)
        lb (count b)]
    (cond
      (= a b)    0
      (zero? la) lb
      (zero? lb) la
      :else
      (let [prev (int-array (range (inc lb)))]
        (dotimes [i la]
          (let [curr (int-array (inc lb))
                ai   (.charAt a (int i))]
            (aset curr 0 (int (inc i)))
            (dotimes [j lb]
              (let [cost (if (= ai (.charAt b (int j))) 0 1)]
                (aset curr (inc j)
                      (int (min (inc (aget curr j))
                                (inc (aget prev (inc j)))
                                (+ (aget prev j) cost))))))
            ;; Clojure interop: (System/arraycopy src srcPos dst dstPos length)
            (System/arraycopy curr 0 prev 0 (inc lb))))
        (aget prev lb)))))

;; ═══════════════════════════════════════════════════════════════════════════
;; THRESHOLD ADAPTATIVO
;; ═══════════════════════════════════════════════════════════════════════════

(defn fuzzy-threshold
  "Umbral de edición adaptativo basado en la longitud del término.
   Términos muy cortos tienen threshold 0 para evitar falsos positivos masivos.
   Términos normales admiten 1-2 errores tipográficos.

   Tabla:
     ≤ 2 chars → 0  ('ID', 'ab' → solo exacto)
     3-5 chars → 1  ('Rak'→'Rack', 'Senso'→'Sensor', threshold protege IDs 3c)
     6-8 chars → 1  ('Chiller'→'Chiler', threshold conservador)
     ≥ 9 chars → 2  ('work order'→'wrk ordeer')"
  ^long [^String term]
  (let [n (count term)]
    (cond
      (<= n 2) 0   ;; 'ID', 'ab' → solo exacto (evita flood de falsos positivos)
      (<= n 5) 1   ;; 'Rak'→'Rack', 'Sensr'→'Sensor', 'Chil'→...
      (<= n 8) 1   ;; 'Chiller'→'Chiler'
      :else    2)));; 'work order'→'wrk ordeer'

;; ═══════════════════════════════════════════════════════════════════════════
;; TOKENIZER — word-level split para fuzzy por palabras
;; ═══════════════════════════════════════════════════════════════════════════

(defn- tokenize
  "Divide un valor en tokens (lowercase) por espacios, guiones y puntos.
   'Chiller A-01' → ['chiller' 'a' '01']
   'Rack Server R-02' → ['rack' 'server' 'r' '02']"
  [^String s]
  (when (seq s)
    (-> s
        (.toLowerCase)
        (.split "[\\s\\-\\._]+"))))

;; ═══════════════════════════════════════════════════════════════════════════
;; FUZZY-MATCH? — función principal
;; ═══════════════════════════════════════════════════════════════════════════

(defn fuzzy-match?
  "True si value contiene term de forma aproximada.

   Pipeline de evaluación (short-circuit):
     1. Guard: nil/blank → false
     2. Fast path: substring case-insensitive (str/includes? lower lower)
     3. Fuzzy path: algún token del valor tiene Levenshtein ≤ threshold(term)

   Ejemplos:
     (fuzzy-match? 'Chiller A-01' 'Chiller') → true  (substring)
     (fuzzy-match? 'Chiller A-01' 'chiller') → true  (case-insensitive)
     (fuzzy-match? 'Chiller A-01' 'Chiler')  → true  (lev=1 ≤ threshold=1)
     (fuzzy-match? 'Chiller A-01' 'Rak')     → false (no matchea)
     (fuzzy-match? 'Rack Server'  'Rak')     → true  (lev=1 token 'rack')
     (fuzzy-match? 'UPS B-12'    'up')       → false (threshold=0 para ≤3)"
  [value term]
  (when (and (string? value) (seq value)
             (string? term)  (seq term))
    (let [v-lower (.toLowerCase ^String value)
          t-lower (.toLowerCase ^String term)
          thresh  (fuzzy-threshold t-lower)]
      (or
       ;; ── Fast path: substring case-insensitive ──────────────────────────
       (.contains v-lower t-lower)

       ;; ── Fuzzy path: word-level Levenshtein ────────────────────────────
       ;; Solo si el término tiene longitud suficiente para threshold > 0
       (when (pos? thresh)
         (let [tokens (tokenize value)]
           (some #(<= (levenshtein-distance % t-lower) thresh)
                 tokens)))))))

;; ═══════════════════════════════════════════════════════════════════════════
;; FUNCIÓN CAPTURADA — para inyectar como :in en queries Datahike
;; ═══════════════════════════════════════════════════════════════════════════

(def fuzzy-match-fn
  "Var capturada de fuzzy-match? apta para pasar como parámetro :in a d/q.
   Datahike acepta funciones Clojure arbitrarias en cláusulas de predicado:
     [(fuzzy-match-fn? ?v term)]
   donde fuzzy-match-fn? es el símbolo y la var se inyecta via :in."
  fuzzy-match?)
