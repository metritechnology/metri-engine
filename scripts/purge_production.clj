(ns purge-production
  (:require [datahike.api :as d]
            [integrant.core :as ig]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [datahike-dynamodb.core]))

;; Cargar config de sistema
(declare resolve-env-var)
(defn load-config []
  (let [f (io/file "resources/config/system.edn")]
    (if (.exists f)
      (edn/read-string {:readers {'env resolve-env-var 
                                  'env-int resolve-env-var 
                                  'env-bool resolve-env-var 
                                  'ig/ref identity}} 
                       (slurp f))
      (throw (ex-info "No se encuentra system.edn" {})))))

;; Resolver #env
(defmethod print-method clojure.lang.TaggedLiteral [tl ^java.io.Writer w]
  (.write w (str "#" (:tag tl) " " (pr-str (:form tl)))))

(defn resolve-env-var [val]
  (let [env-val (System/getenv val)]
    (if (nil? env-val)
      val
      env-val)))

(defn purge-db! []
  (println "⚠️  INICIANDO PURGA DE BASE DE DATOS DE PRODUCCIÓN ⚠️")
  (let [cfg (load-config)
        dh-cfg (get cfg :infra/datahike)
        store (:store dh-cfg)]
    
    (if-not store
      (println "❌ Configuración de Datahike no encontrada en system.edn.")
      (let [table (resolve-env-var (:table store))
            region (resolve-env-var (:region store))
            resolved-cfg {:store {:backend :dynamodb
                                  :table table
                                  :region region}}]
        (println "Conectando a DynamoDB:")
        (println "  Región:" region)
        (println "  Tabla :" table)
        
        (if (or (nil? table) (nil? region))
          (println "❌ Faltan variables de entorno AWS_REGION o DATAHIKE_DDB_TABLE.")
          (try
            (if (d/database-exists? resolved-cfg)
              (do
                (println "⏳ Base de datos encontrada. Purgando...")
                (d/delete-database resolved-cfg)
                (println "✅ Purga completada exitosamente. Todos los datos han sido eliminados."))
              (println "ℹ️ La base de datos no existe o ya fue purgada."))
            (catch Exception e
              (println "❌ Error al purgar la base de datos:")
              (println (.getMessage e))
              (println "Verifica tus credenciales de AWS (ej. aws configure)."))))))))

(purge-db!)
