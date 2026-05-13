(ns metri.aegis.sql.helpers
  "Utilidades de normalización de nombres compartidas por todos los compiladores SQL.
   SRP: conversión de tipos — sin lógica de negocio, sin I/O."
  (:require [clojure.string :as str]))

(defn col-kw
  "field → keyword con underscores para HoneySQL → columna SQL sin quoting.
   :work_order/status   → :status
   :entity/tenant-id    → :tenant_id"
  [field]
  (if (= field :tenant/id)
    :_tenant
    (keyword (-> (if (keyword? field) (name field) (str field))
                 (str/replace "-" "_")))))

(defn table-str
  "Genera string table-name. (Ej: metri_olap.meter_reading).
   Usa en :from como (keyword (table-str entity db)) para evitar quoting."
  [entity database]
  (str database "." (str/replace (str entity) "-" "_")))
