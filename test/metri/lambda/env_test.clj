(ns metri.lambda.env-test
  "Matriz TDD — ENV-01..06 (01.01 §8.5 template.yaml)
   Valida que las variables de entorno críticas declaradas en el SAM template
   estén presentes y bien formadas cuando el proceso corre en 'production'.

   En entorno local/CI las variables pueden estar ausentes — los tests emiten
   advertencia pero NO fallan (placeholder <unset:VAR>).

   SSOT: template.yaml §8.5 / 01_FASE_ALISTAMIENTO_ENTORNO.md §8.5

   Ejecución:
     - Normal (CI / local):  clojure -M:test              ← siempre pasan
     - Producción simulada:  ENVIRONMENT=production \\
                             OUTBOX_QUEUE_URL=https://... \\
                             clojure -M:test               ← validan valor real"
  (:require [clojure.test :refer [deftest is testing]]))

;; ─── Helpers ─────────────────────────────────────────────────────────────────

(defn- env
  "Obtiene la variable de entorno. Retorna nil si no está presente."
  [var-name]
  (System/getenv var-name))

(defn- production?
  "Retorna true solo cuando ENVIRONMENT=production."
  []
  (= "production" (env "ENVIRONMENT")))

(defn- present?
  "Retorna true si el valor no es nil y no es un placeholder <unset:...>."
  [v]
  (and (string? v)
       (not (clojure.string/blank? v))
       (not (clojure.string/starts-with? v "<unset:"))))

(defmacro env-assertion
  "Macro de conveniencia: en producción falla; en otros entornos emite
   un mensaje informativo y pasa (is true)."
  [var-name pred description]
  `(let [v#   (env ~var-name)
         ok?# (~pred v#)]
     (if (production?)
       (is ok?# (str ~var-name " — " ~description " (valor: " v# ")"))
       (do
         (when-not ok?#
           (println (str "  [ENV-WARN] " ~var-name " ausente/inválida en entorno local — OK en prod")))
         (is true (str ~var-name " — skip en entorno local"))))))

;; ─── ENV-01..06 ──────────────────────────────────────────────────────────────

(deftest env-01-outbox-queue-url
  "ENV-01 — OUTBOX_QUEUE_URL presente y tiene forma de URL SQS válida
   Origen: template.yaml !GetAtt MetriOutboxQueue.QueueUrl
   Esperado: https://sqs.{region}.amazonaws.com/{account}/{queue}.fifo"
  (env-assertion
    "OUTBOX_QUEUE_URL"
    (fn [v]
      (and (present? v)
           (or (clojure.string/starts-with? v "https://sqs.")
               ;; Permite URL de ElasticMQ local en tests de integración
               (clojure.string/starts-with? v "http://localhost")
               (clojure.string/starts-with? v "http://metri-elasticmq"))))
    "debe ser una URL SQS válida (https://sqs.*.amazonaws.com/...)"))

(deftest env-02-cedar-policies-table
  "ENV-02 — CEDAR_POLICIES_TABLE presente y no vacío
   Origen: template.yaml !Ref MetriSchemasTable
   Propósito: tabla DynamoDB de esquemas JSON y políticas Cedar ABAC"
  (env-assertion
    "CEDAR_POLICIES_TABLE"
    present?
    "debe ser el nombre de la tabla DynamoDB de políticas Cedar"))

(deftest env-03-datahike-ddb-table
  "ENV-03 — DATAHIKE_DDB_TABLE presente y no vacío
   Origen: template.yaml !Ref DatahikeTableName (default: metri-datahike-prod)
   Propósito: tabla DynamoDB del store transaccional Datahike Serverless"
  (env-assertion
    "DATAHIKE_DDB_TABLE"
    present?
    "debe ser el nombre de la tabla DynamoDB de Datahike"))

(deftest env-04-valkey-host
  "ENV-04 — VALKEY_HOST presente y parece un hostname o IP válido
   Origen: template.yaml !GetAtt MetriValkeyCache.Endpoint.Address
   Propósito: endpoint privado del cluster Valkey Serverless (VPC-only)"
  (env-assertion
    "VALKEY_HOST"
    (fn [v]
      (and (present? v)
           ;; Hostname real de AWS: *.cache.amazonaws.com
           ;; o nombre Docker local: metri-valkey-local
           (re-matches #"[a-zA-Z0-9]([a-zA-Z0-9\-\.]+)?[a-zA-Z0-9]" v)))
    "debe ser un hostname o IP válido (sin esquema)"))

(deftest env-05-valkey-port
  "ENV-05 — VALKEY_PORT presente y parseable como entero en rango [1-65535]
   Origen: template.yaml !GetAtt MetriValkeyCache.Endpoint.Port
   Valor esperado en producción: 6379"
  (let [raw (env "VALKEY_PORT")]
    (if (production?)
      (testing "En producción: VALKEY_PORT debe ser un int válido"
        (is (present? raw) "VALKEY_PORT debe estar presente")
        (let [port (try (Integer/parseInt raw) (catch Exception _ nil))]
          (is (some? port)    "VALKEY_PORT debe ser parseable como entero")
          (is (< 0 port 65536) "VALKEY_PORT debe estar en rango [1, 65535]")
          (is (= 6379 port)  "VALKEY_PORT esperado es 6379 (Valkey default)")))
      (do
        (when (and raw (not (present? raw)))
          (println "  [ENV-WARN] VALKEY_PORT ausente/inválida en entorno local — OK en prod"))
        (is true "VALKEY_PORT — skip en entorno local")))))

(deftest env-06-metri-origin-token
  "ENV-06 — METRI_ORIGIN_TOKEN presente y no vacío (Zero-Trust perimetral)
   Origen: template.yaml !Ref AWS::StackId
   Propósito: token inyectado por CloudFront; Lambda lo valida en cada request.
   CRÍTICO: Si está ausente en producción, el handler acepta requests sin validar."
  (env-assertion
    "METRI_ORIGIN_TOKEN"
    present?
    "CRÍTICO — debe ser el StackId de CloudFormation (ARN único e inmutable)"))

;; ─── Validación cruzada ───────────────────────────────────────────────────────

(deftest env-cross-valkey-host-port-consistency
  "ENV-CROSS — VALKEY_HOST y VALKEY_PORT deben estar presentes o ausentes juntos"
  (let [host (env "VALKEY_HOST")
        port (env "VALKEY_PORT")]
    (if (production?)
      (is (= (present? host) (present? port))
          "VALKEY_HOST y VALKEY_PORT deben estar ambos presentes en producción")
      (is true "Consistencia Valkey — skip en entorno local"))))

(deftest env-cross-all-required-in-production
  "ENV-CROSS-ALL — En producción, TODAS las vars críticas deben estar presentes"
  (let [required-vars ["OUTBOX_QUEUE_URL"
                       "CEDAR_POLICIES_TABLE"
                       "DATAHIKE_DDB_TABLE"
                       "VALKEY_HOST"
                       "VALKEY_PORT"
                       "METRI_ORIGIN_TOKEN"]
        missing (filterv #(not (present? (env %))) required-vars)]
    (if (production?)
      (is (empty? missing)
          (str "Variables críticas ausentes en producción: " (clojure.string/join ", " missing)))
      (do
        (when (seq missing)
          (println (str "  [ENV-INFO] Variables no configuradas (OK en local): "
                        (clojure.string/join ", " missing))))
        (is true "Vars de producción — skip en entorno local")))))
