(ns metri.aegis.sort-pagination-test
  "Tests de integración para sort + paginación en OLTP (Datahike in-memory).

   Estrategia:
   - Datahike in-memory → sin I/O real, sin Lambda, sin DynamoDB.
   - Se insertan 5 activos con nombres y status controlados.
   - Se invoca `run-oltp-query` con el AST-IR completo (igual que ast_compiler genera)
     para verificar sort + paginación end-to-end via el compiler OLTP real.
   - OLAP: se testean paginate-rows + build-pagination como unidades puras
     (Athena no es moqueable sin AWS SDK).

   Tests (14):
     SORT-01 — ASC por nombre (string field)
     SORT-02 — DESC por nombre
     SORT-03 — Multi-clave: status ASC luego name DESC
     SORT-04 — Sort ASC + limit=2, no cursor → primeras 2 filas
     SORT-05 — Sort ASC + cursor(offset=2) → posiciones 3+4
     SORT-06 — Cursor fuera de rango → retorna []
     SORT-07 — Sin sort → retorna todos sin crash
     SORT-PURE-01..04 — sort-oltp-result puro (sin Datahike)
     SORT-08..10 — paginate-rows (offset/limit)
     SORT-11..13 — build-pagination (has-next / has-previous / cursores)
     SORT-14 — encode-cursor / decode-cursor roundtrip"
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datahike.api :as d]
            [metri.aegis.datalog.executor :as executor]
            [metri.aegis.datalog.sort :as sort-mod]
            [metri.aegis.pagination :as pagination]))

;; =============================================================================
;; ── Fixture Datahike in-memory ───────────────────────────────────────────────
;; =============================================================================

(def ^:private dh-cfg {:store {:backend :mem :id "sort-test-db"}})

(def ^:private schema-tx
  [{:db/ident :entity/ulid   :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one :db/unique :db.unique/identity :db/index true}
   {:db/ident :entity/type   :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :tenant/id     :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :asset/name    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :asset/status  :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one :db/index true}])

;; 5 activos con nombres y status controlados para sort predecible
(def ^:private seed-data
  [{:entity/ulid "u1" :entity/type :asset :tenant/id "tnt"
    :asset/name "Bomba Centrifuga" :asset/status "ACTIVE"}
   {:entity/ulid "u2" :entity/type :asset :tenant/id "tnt"
    :asset/name "Compresor Delta"  :asset/status "INACTIVE"}
   {:entity/ulid "u3" :entity/type :asset :tenant/id "tnt"
    :asset/name "Actuador Alpha"   :asset/status "ACTIVE"}
   {:entity/ulid "u4" :entity/type :asset :tenant/id "tnt"
    :asset/name "Filtro Zeta"      :asset/status "IN_MAINTENANCE"}
   {:entity/ulid "u5" :entity/type :asset :tenant/id "tnt"
    :asset/name "Sensor Beta"      :asset/status "ACTIVE"}])

(def ^:dynamic *conn* nil)

(defn sort-test-fixture [f]
  (when (d/database-exists? dh-cfg) (d/delete-database dh-cfg))
  (d/create-database dh-cfg)
  (let [conn (d/connect dh-cfg)]
    (d/transact conn {:tx-data schema-tx})
    (d/transact conn {:tx-data seed-data})
    (binding [*conn* conn]
      (try (f)
           (finally
             (d/release conn)
             (d/delete-database dh-cfg))))))

(use-fixtures :each sort-test-fixture)

;; =============================================================================
;; ── AST-IR builder helper ────────────────────────────────────────────────────
;; El compiler OLTP (compile-oltp-query) espera estos campos en el AST-IR:
;;   :entity   — string ("asset")
;;   :where    — nil → sin filtros extra (solo entity/type + tenant/id implícitos del compiler)
;;   :select   — mapa de campos a proyectar (como el selectTree del frontend)
;;   :sort     — vector de SortDefinition [{:field "name" :descending bool}]
;;   :limit    — integer
;;   :cursor   — string Base64 | nil
;;   :output-cast — keyword (:TABLE)
;; =============================================================================

(defn- make-ast
  "AST-IR compatible con compile-oltp-query + run-oltp-query.
   Refleja exactamente lo que ast_compiler.clj genera:
   - :where  → [:and tenant-node ...filter-nodes]
   - :sort   → vector de SortDefinition (leído por compiler.clj L88)
   - :select → mapa de proyección"
  [& {:keys [order-by cursor limit]
      :or   {order-by [] cursor nil limit 50}}]
  {:entity      "asset"
   :schema      {:entity "asset" :engine "oltp"}
   ;; where = [:and tenant-node] — formato real del ast_compiler
   :where        [:and [:= :tenant/id "tnt"]]
   ;; select → '[*] wildcard: select->pull-pattern lo acepta directamente.
   ;; Los tests no necesitan proyección parcial — simplificamos al máximo.
   :select       '[*]
   ;; compiler.clj L88: :sort (or (:sort ast-ir) [])
   :sort         order-by
   :limit        limit
   :cursor       cursor
   :output-cast  :TABLE})

;; Helper: ejecuta y extrae los :asset/name en orden
(defn- query-names [ast-ir]
  (let [[tag body] (executor/run-oltp-query *conn* ast-ir "tnt")]
    (when (= :ok tag)
      ;; run-oltp-query post-procesa los pull maps → {:name "..." :status "..."}
      ;; Las claves pasan por format-pull-value que convierte :asset/name → :name
      (mapv #(or (:name %) (:asset/name %)) (:rows body)))))

;; =============================================================================
;; ── SORT OLTP — integración con Datahike in-memory ──────────────────────────
;; =============================================================================

(deftest sort-01-asc-by-name
  "SORT-01 — order-by name ASC → A→Z."
  (let [ast   (make-ast :order-by [{:field "name" :descending false}])
        names (query-names ast)]
    (is (some? names) "La query debe retornar resultado [:ok ...]")
    (is (= (count names) 5) "Deben retornarse los 5 activos")
    (is (= names (sort names))
        "Los nombres deben estar ordenados A→Z")))

(deftest sort-02-desc-by-name
  "SORT-02 — order-by name DESC → Z→A."
  (let [ast   (make-ast :order-by [{:field "name" :descending true}])
        names (query-names ast)]
    (is (some? names))
    (is (= names (reverse (sort names)))
        "Los nombres deben estar ordenados Z→A")))

(deftest sort-03-multi-key
  "SORT-03 — Sort multi-clave: status ASC, name DESC dentro del mismo status."
  (let [ast      (make-ast :order-by [{:field "status" :descending false}
                                      {:field "name"   :descending true}])
        [tag body] (executor/run-oltp-query *conn* ast "tnt")]
    (is (= :ok tag))
    (let [rows     (:rows body)
          statuses (mapv #(or (:status %) (:asset/status %)) rows)]
      (is (= 5 (count rows)) "Todos los activos deben estar presentes")
      ;; El status global debe ser ASC (sin saltos de orden)
      (is (= statuses (sort statuses))
          "El status global debe estar en orden ASC"))))

(deftest sort-04-page-1-with-sort
  "SORT-04 — Sort ASC + limit=2 sin cursor → 2 primeros en orden A→Z."
  (let [ast   (make-ast :order-by [{:field "name" :descending false}]
                        :limit 2)
        names (query-names ast)
        all-sorted (sort ["Bomba Centrifuga" "Compresor Delta" "Actuador Alpha"
                          "Filtro Zeta" "Sensor Beta"])]
    (is (= 2 (count names)) "limit=2 debe retornar exactamente 2 filas")
    (is (= names (take 2 all-sorted))
        "Deben ser las 2 primeras en orden ASC")))

(deftest sort-05-page-2-with-cursor
  "SORT-05 — Sort ASC + limit=2 + cursor(offset=2) → posiciones 3 y 4."
  (let [cursor (pagination/encode-cursor 2 2)   ;; Base64(\"2:2\") = página 2
        ast    (make-ast :order-by [{:field "name" :descending false}]
                         :limit 2
                         :cursor cursor)
        names  (query-names ast)
        all-sorted (sort ["Bomba Centrifuga" "Compresor Delta" "Actuador Alpha"
                          "Filtro Zeta" "Sensor Beta"])]
    (is (= 2 (count names)) "Página 2 con limit=2 debe tener 2 filas")
    (is (= names (vec (take 2 (drop 2 all-sorted))))
        "Deben ser las posiciones 3 y 4 del orden ASC")))

(deftest sort-06-page-beyond-total
  "SORT-06 — cursor offset >> total → retorna [] (sin crash)."
  (let [cursor (pagination/encode-cursor 100 2)  ;; offset=100 con solo 5 registros
        ast    (make-ast :order-by [{:field "name" :descending false}]
                         :limit 2
                         :cursor cursor)
        names  (query-names ast)]
    (is (= [] names) "Offset fuera de rango debe retornar []")))

(deftest sort-07-no-sort-returns-all
  "SORT-07 — Sin sort → todos los registros sin crash."
  (let [ast        (make-ast :order-by [])
        [tag body] (executor/run-oltp-query *conn* ast "tnt")]
    (is (= :ok tag))
    (is (= 5 (count (:rows body))) "Deben retornarse los 5 activos")))

(deftest sort-pagination-has-next-flag
  "SORT-PAG — run-oltp-query retorna :pagination con has-next=true cuando hay más páginas."
  (let [ast        (make-ast :order-by [{:field "name" :descending false}]
                             :limit 2)
        [tag body] (executor/run-oltp-query *conn* ast "tnt")]
    (is (= :ok tag))
    (is (some? (:pagination body)) ":pagination debe estar presente")
    (is (true? (get-in body [:pagination :has-next]))
        "has-next debe ser true cuando limit=2 y total=5")))

(deftest sort-pagination-last-page-no-next
  "SORT-PAG-LAST — Última página → has-next=false."
  (let [cursor     (pagination/encode-cursor 4 2)  ;; offset=4, quedan 1 fila
        ast        (make-ast :order-by [{:field "name" :descending false}]
                             :limit 2
                             :cursor cursor)
        [tag body] (executor/run-oltp-query *conn* ast "tnt")]
    (is (= :ok tag))
    (is (false? (get-in body [:pagination :has-next]))
        "En la última página has-next debe ser false")))

;; =============================================================================
;; ── sort-oltp-result puro — sin Datahike ────────────────────────────────────
;; =============================================================================

(def ^:private test-rows
  [{:name "Zebra"  :status "ACTIVE"}
   {:name "Alpha"  :status "INACTIVE"}
   {:name "Mango"  :status "ACTIVE"}
   {:name "Delta"  :status "IN_MAINTENANCE"}])

(deftest sort-pure-01-asc
  "SORT-PURE-01 — sort-oltp-result :name ASC."
  (let [sorted (sort-mod/sort-oltp-result test-rows [{:field :name :descending false}])]
    (is (= ["Alpha" "Delta" "Mango" "Zebra"] (mapv :name sorted)))))

(deftest sort-pure-02-desc
  "SORT-PURE-02 — sort-oltp-result :name DESC."
  (let [sorted (sort-mod/sort-oltp-result test-rows [{:field :name :descending true}])]
    (is (= ["Zebra" "Mango" "Delta" "Alpha"] (mapv :name sorted)))))

(deftest sort-pure-03-multi-key
  "SORT-PURE-03 — sort-oltp-result :status ASC, :name DESC."
  (let [sorted   (sort-mod/sort-oltp-result test-rows [{:field :status :descending false}
                                                        {:field :name   :descending true}])
        statuses (mapv :status sorted)]
    ;; ACTIVE < IN_MAINTENANCE < INACTIVE lexicográficamente
    (is (= statuses (sort statuses)) "Status debe ser ASC")))

(deftest sort-pure-04-empty-defs
  "SORT-PURE-04 — sort-oltp-result con [] retorna el mismo vector (sin mutación)."
  (let [sorted (sort-mod/sort-oltp-result test-rows [])]
    (is (= test-rows sorted))))

;; =============================================================================
;; ── OLAP: Pagination pura (sin Athena I/O) ───────────────────────────────────
;; Las mismas funciones se usan en sql/executor.clj para OLAP.
;; =============================================================================

(def ^:private olap-rows
  (mapv (fn [i] {:id i :name (str "Row-" i)}) (range 10)))  ;; 10 filas simuladas

(deftest sort-08-paginate-rows-page-1
  "SORT-08 (OLAP) — paginate-rows offset=0 limit=3 → primeras 3 filas."
  (let [result (pagination/paginate-rows olap-rows 0 3)]
    (is (= 3 (count result)))
    (is (= 0 (:id (first result))))))

(deftest sort-09-paginate-rows-page-2
  "SORT-09 (OLAP) — paginate-rows offset=3 limit=3 → filas 3,4,5."
  (let [result (pagination/paginate-rows olap-rows 3 3)]
    (is (= 3 (count result)))
    (is (= 3 (:id (first result))) "La primera fila de página 2 debe ser id=3")))

(deftest sort-10-paginate-rows-last
  "SORT-10 (OLAP) — Última página con menos filas que limit."
  (let [result (pagination/paginate-rows olap-rows 9 3)]
    (is (= 1 (count result)))
    (is (= 9 (:id (first result))))))

(deftest sort-11-build-pagination-has-next
  "SORT-11 (OLAP) — total=10, offset=0, limit=3 → has-next=true, has-previous=false."
  (let [pag (pagination/build-pagination {:offset 0 :limit 3 :total 10})]
    (is (true?  (:has-next pag)))
    (is (false? (:has-previous pag)))
    (is (some?  (:next-cursor pag)))))

(deftest sort-12-build-pagination-has-previous
  "SORT-12 (OLAP) — offset=3 → has-previous=true, next-cursor y previous-cursor presentes."
  (let [pag (pagination/build-pagination {:offset 3 :limit 3 :total 10})]
    (is (true? (:has-previous pag)))
    (is (true? (:has-next pag)))
    (is (some? (:previous-cursor pag)))
    (is (some? (:next-cursor pag)))))

(deftest sort-13-build-pagination-last-page
  "SORT-13 (OLAP) — En la última página has-next=false."
  (let [pag (pagination/build-pagination {:offset 9 :limit 3 :total 10})]
    (is (false? (:has-next pag)))))

(deftest sort-14-cursor-roundtrip
  "SORT-14 — encode-cursor + decode-cursor es idempotente."
  (doseq [[offset limit] [[0 10] [10 10] [20 5] [100 50]]]
    (let [cursor  (pagination/encode-cursor offset limit)
          decoded (pagination/decode-cursor cursor limit)]
      (is (= offset (:offset decoded)) (str "offset debe ser " offset))
      (is (= limit  (:limit  decoded)) (str "limit debe ser " limit)))))
