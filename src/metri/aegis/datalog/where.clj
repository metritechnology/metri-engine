(ns metri.aegis.datalog.where
  "Compilador WHERE: 14 FilterOperator + :ref-filter AST IR → cláusulas Datalog Datahike.
   SRP: transpilación de predicados — sin I/O, sin estado.

   FilterOperator cubiertos (contrato §1):
     =, not=, >, <, >=, <=           → comparaciones directas
     :in, :not-in                    → set binding via :in
     :between                        → rango numérico inclusivo [min, max]
     :matches                        → re-find con re-pattern (regex)
     :like                           → LIKE SQL → regex anclado (^...$)
     :contains                       → clojure.string/includes? (exacto, case-sensitive)
     :is-null                        → (missing? $ entity-var attr)
     :is-not-null                    → [entity-var attr ?v] (existencia implícita)
     :fts                            → regex case-insensitive (?i) best-effort (deprecated)
     :fuzzy                          → Levenshtein word-level + substring case-insensitive
     :and, :or, :not                 → operadores lógicos Datalog

   Extensión cross-entity:
     :ref-filter [ref-field inner-node]
       → traversa la referencia y aplica inner-node sobre la entidad relacionada
       → soporta anidamiento arbitrario y cualquier FilterOperator en inner-node

   Ejemplo :ref-filter:
     [:ref-filter \"location_id\"
       [:and [:= \"type\" \"BUILDING\"] [:> \"area_value\" 100]]]

     Genera:
       [?e      :location_id  ?ref_1]
       [?ref_1  :type         \"BUILDING\"]
       [?ref_1  :area_value   ?v_2]
       [(> ?v_2 100)]

   Anidamiento arbitrario:
     [:ref-filter \"location_id\"
       [:ref-filter \"parent_location_id\"
         [:= \"type\" \"SITE\"]]]

     Genera:
       [?e     :location_id        ?ref_1]
       [?ref_1 :parent_location_id ?ref_2]
       [?ref_2 :type               \"SITE\"]"
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [metri.aegis.datalog.fuzzy :as fz]))

;; ── Helpers ───────────────────────────────────────────────────────────────────

(defn make-var
  "Genera un símbolo Datahike único (?prefix_N) con contador atómico compartido."
  [counter prefix]
  (symbol (str "?" prefix (swap! counter inc))))

(defn- like->regex
  "Convierte un patrón SQL LIKE → regex anclado ^...$
   Reglas: % → .* | _ → . | metacaracteres regex escapados."
  [pattern]
  (str "^"
       (-> pattern
           (str/replace #"([\.\+\*\^\$\{\}\(\)\|\[\]])" "\\\\$1")
           (str/replace "%" ".*")
           (str/replace "_" "."))
       "$"))

;; ── Compilador interno (entity-var parametrizable) ───────────────────────────

(declare compile-node)

(defn- compile-node
  "Compilador recursivo interno.
   entity-var: símbolo Datalog que representa la entidad actual (default '?e).
   Permite que :ref-filter cambie la entidad raíz para las cláusulas del subárbol."
  [node counter entity-var]
  (when (nil? node)
    (throw (ex-info "Null where node" {:code :AEG_COMPILE_001 :detail "nil node"})))
  (let [[op & args] node]
    (case op

      ;; ── Comparación simple ────────────────────────────────────────────────
      :=    (let [[field val] args]
              {:clauses [[entity-var field val]] :in-sym->val {}})

      :not= (let [[field val] args
                  sym (make-var counter "v")]
              {:clauses [[entity-var field sym] [(list 'not= sym val)]]
               :in-sym->val {}})

      :>    (let [[field val] args
                  sym (make-var counter "v")]
              {:clauses [[entity-var field sym] [(list '> sym val)]]
               :in-sym->val {}})

      :<    (let [[field val] args
                  sym (make-var counter "v")]
              {:clauses [[entity-var field sym] [(list '< sym val)]]
               :in-sym->val {}})

      :>=   (let [[field val] args
                  sym (make-var counter "v")]
              {:clauses [[entity-var field sym] [(list '>= sym val)]]
               :in-sym->val {}})

      :<=   (let [[field val] args
                  sym (make-var counter "v")]
              {:clauses [[entity-var field sym] [(list '<= sym val)]]
               :in-sym->val {}})

      ;; ── Pertenencia (IN) — binding de set via :in ─────────────────────────
      :in     (let [[field vals] args
                    val-sym (make-var counter "v")
                    set-sym (make-var counter "set")]
                {:clauses     [[entity-var field val-sym]
                               [(list 'contains? set-sym val-sym)]]
                 :in-sym->val {set-sym (set vals)}})

      :not-in (let [[field vals] args
                    val-sym (make-var counter "v")
                    set-sym (make-var counter "set")]
                {:clauses     [[entity-var field val-sym]
                               [(list 'not (list 'contains? set-sym val-sym))]]
                 :in-sym->val {set-sym (set vals)}})

      ;; ── BETWEEN — rango numérico inclusivo ───────────────────────────────
      :between (let [[field [min-v max-v]] args
                     sym (make-var counter "v")]
                 {:clauses     [[entity-var field sym]
                                [(list '>= sym min-v)]
                                [(list '<= sym max-v)]]
                  :in-sym->val {}})

      ;; ── MATCHES — expresión regular (re-find) ─────────────────────────────
      :matches (let [[field pattern] args
                     sym (make-var counter "v")
                     regex-sym (make-var counter "regex")]
                 {:clauses     [[entity-var field sym]
                                [(list 're-find regex-sym sym)]]
                  :in-sym->val {regex-sym (re-pattern pattern)}})

      ;; ── LIKE — SQL LIKE convertido a regex anclado ────────────────────────
      :like   (let [[field pattern] args
                    sym   (make-var counter "v")
                    regex-sym (make-var counter "regex")
                    regex (like->regex pattern)]
                {:clauses     [[entity-var field sym]
                               [(list 're-find regex-sym sym)]]
                 :in-sym->val {regex-sym (re-pattern regex)}})

      ;; ── CONTAINS — subcadena (clojure.string/includes?) ──────────────────
      :contains (let [[field val] args
                      sym (make-var counter "v")]
                  {:clauses     [[entity-var field sym]
                                 [(list 'clojure.string/includes? sym val)]]
                   :in-sym->val {}})

      ;; ── IS NULL — atributo ausente en Datahike (missing?) ─────────────────
      :is-null (let [[field _] args]
                 {:clauses     [[(list 'missing? '$ entity-var field)]]
                  :in-sym->val {}})

      ;; ── IS NOT NULL — atributo presente (existencia implícita via binding) ─
      :is-not-null (let [[field _] args
                         sym (make-var counter "v")]
                     {:clauses     [[entity-var field sym]]
                      :in-sym->val {}})

      ;; ── FTS — full-text search best-effort (regex case-insensitive) ───────
      :fts (let [[field val] args
                 sym (make-var counter "v")
                 regex-sym (make-var counter "regex")]
             (log/warn "[Aegis Datalog] FTS no nativo en Datahike — regex (?i) para:"
                       field "| término:" val)
             {:clauses     [[entity-var field sym]
                            [(list 're-find regex-sym sym)]]
              :in-sym->val {regex-sym (re-pattern (str "(?i)" val))}})

      ;; ── FUZZY — Levenshtein word-level + substring case-insensitive ─────────
      ;;
      ;; Combina dos estrategias en short-circuit:
      ;;   1. Fast path: (.contains (lower v) (lower term))  — O(n)
      ;;   2. Fuzzy path: Levenshtein(token, term) ≤ threshold  — O(m·n) por token
      ;;
      ;; Threshold adaptativo (fz/fuzzy-threshold):
      ;;   ≤ 3 chars → 0  (sin fuzzy — evita falsos positivos en IDs cortos)
      ;;   4-8 chars → 1  ('Chiller'→'Chiler', 'Rack'→'Rak')
      ;;   ≥ 9 chars → 2  ('work order'→'wrk ordeer')
      ;;
      ;; fuzzy-match-fn se inyecta via :in → Datahike la evalúa como predicado
      ;; sin necesidad de resolver símbolos globales en el entorno del motor.
      :fuzzy (let [[field val] args
                   sym        (make-var counter "v")
                   fn-sym     (make-var counter "fuzz")]
               (log/debug "[Aegis Datalog] :fuzzy | campo:" field "| término:" val
                          "| threshold:" (fz/fuzzy-threshold (str val)))
               {:clauses     [[entity-var field sym]
                              [(list fn-sym sym (str val))]]
                :in-sym->val {fn-sym fz/fuzzy-match-fn}})

      ;; ── REF-FILTER — cross-entity join via referencia ────────────────────
      ;;
      ;; [:ref-filter "location_id" inner-node]
      ;;
      ;; Genera:
      ;;   [entity-var :location_id ?ref_N]   ← traversal de la referencia
      ;;   + cláusulas de inner-node usando ?ref_N como nueva entity-var
      ;;
      ;; Soporta anidamiento arbitrario:
      ;;   [:ref-filter "location_id"
      ;;     [:ref-filter "parent_location_id"
      ;;       [:= "type" "SITE"]]]
      ;;
      ;; Genera:
      ;;   [?e     :location_id        ?ref_1]
      ;;   [?ref_1 :parent_location_id ?ref_2]
      ;;   [?ref_2 :type               "SITE"]
      :ref-filter
      (let [[ref-field inner-node] args
            ref-sym  (make-var counter "ref")
            ;; ref-field es un keyword namespaced generado por Janus
            ;; (e.g. :asset/location_id) — se usa directamente como atributo Datahike.
            ;; Cláusula de traversal: entity-var → ref-sym via ref-field
            ref-clause [entity-var ref-field ref-sym]
            ;; Compilar inner-node usando ref-sym como la nueva entidad raíz
            inner    (compile-node inner-node counter ref-sym)]
        {:clauses     (into [ref-clause] (:clauses inner))
         :in-sym->val (:in-sym->val inner)})

      ;; ── Lógica booleana ──────────────────────────────────────────────────
      :and (reduce (fn [acc sub]
                     (let [r (compile-node sub counter entity-var)]
                       {:clauses     (into (:clauses acc) (:clauses r))
                        :in-sym->val (merge (:in-sym->val acc) (:in-sym->val r))}))
                   {:clauses [] :in-sym->val {}}
                   args)

      ;; or-join declara entity-var para que Datahike sepa qué variable
      ;; es compartida entre ramas y el contexto externo.
      ;;
      ;; INVARIANTE: cada rama del or-join con múltiples cláusulas DEBE estar
      ;; envuelta en (and ...). Datahike no puede parsear ramas multi-cláusula
      ;; sin el wrapper and → lanza AEG_COMPILE_001 "Cannot parse binding".
      ;;
      ;; Rama simple  (1 cláusula) → [?e :field val]         (sin and)
      ;; Rama compuesta (2+ cláusulas) → (and [?e :f ?v] [(> ?v x)]) (con and)
      :or  (let [sub-results  (mapv #(compile-node % counter entity-var) args)
                 or-branches  (mapv (fn [r]
                                      (let [cls (:clauses r)]
                                        (if (= 1 (count cls))
                                          (first cls)            ;; rama simple
                                          (into ['and] cls))))   ;; rama compuesta
                                    sub-results)
                 or-form      (into ['or-join [entity-var]] or-branches)]
             {:clauses     [or-form]
              :in-sym->val (apply merge (map :in-sym->val sub-results))})

      :not (let [r (compile-node (first args) counter entity-var)]
             {:clauses     [(into ['not] (:clauses r))]
              :in-sym->val (:in-sym->val r)})

      ;; Operador desconocido — falla rápido
      (throw (ex-info (str "Unsupported AST operator for Datahike: " op)
                      {:code :AEG_COMPILE_001 :operator op})))))

;; ── API pública ───────────────────────────────────────────────────────────────

(defn where-node->parts
  "Transpila un nodo del árbol :where AST IR → {:clauses [...] :in-sym->val {...}}.
   Railway puro — lanza ExceptionInfo en nodo inválido, nunca retorna nil.

   Punto de entrada público: usa '?e como entity-var raíz.
   :ref-filter cambia internamente el entity-var para sus subárboles."
  [node counter]
  (compile-node node counter '?e))
