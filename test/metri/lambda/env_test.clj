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

(deftest env-04-hmac-secret-arn
  "ENV-04 — HMAC_SECRET_ARN presente y tiene forma de ARN de Secrets Manager
   Origen: template.yaml !Ref MetriHMACSecret
   Propósito: ARN del secret HMAC-SHA256 para firma de tokens Metri.
   CRITICO: Sin este secret, la Lambda no puede verificar ningún token."
  (env-assertion
    "HMAC_SECRET_ARN"
    (fn [v]
      (and (present? v)
           (clojure.string/starts-with? v "arn:aws:secretsmanager:")))
    "debe ser un ARN de Secrets Manager válido"))

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

(deftest env-cross-all-required-in-production
  "ENV-CROSS-ALL — En producción, TODAS las vars críticas deben estar presentes"
  (let [required-vars ["OUTBOX_QUEUE_URL"
                       "CEDAR_POLICIES_TABLE"
                       "DATAHIKE_DDB_TABLE"
                       "HMAC_SECRET_ARN"
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
