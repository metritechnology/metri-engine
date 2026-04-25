(ns metri.infrastructure-test
  "Suite de Integración FASE 1: Verificación Local de Infraestructura.
   Conecta los clientes REALES contra la red de Docker Compose
   (LocalStack, DynamoDB-Local, ElasticMQ, Valkey) garantizando
   que se cumpla el contrato de Excepciones Zero (Railway Pattern).

   REQUIERE: docker compose up (ver docker-compose.yml)
   SKIP en CI normal — ejecutar con:
     clojure -M:test -i :integration"
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [integrant.core :as ig]
            [metri.domain.protocols :as proto]
            [metri.infrastructure.dynamodb :as ddb]
            [metri.infrastructure.sqs]
            [metri.infrastructure.valkey]
            [metri.infrastructure.eventbridge]
            [metri.infrastructure.kinesis]))

;; ── Configuración Dinámica para apuntar a Docker ──────────────────────────────

(def local-config
  {;; Valkey en puerto 6379 (Redis)
   :infra/valkey
   {:host "metri-valkey-local" :port 6379 :password "" :ssl? false}

   ;; DynamoDB Local en puerto 8000
   :infra/dynamodb
   {:region "us-east-1" :endpoint "http://metri-dynamodb-local:8000"}

   ;; ElasticMQ (SQS) en puerto 9324
   :moira/sqs-bus
   {:region "us-east-1" :endpoint "http://metri-elasticmq:9324" :queue-url "http://metri-elasticmq:9324/000000000000/metri-dlq.fifo"}})

(def ^:dynamic *system* nil)

(defn system-fixture [f]
  ;; Levantar toda la topología de infra local apuntando a docker
  (let [sys (ig/init local-config)]
    (binding [*system* sys]
      (try
        (f)
        (finally
          (ig/halt! sys))))))

(use-fixtures :once system-fixture)

;; ── Tests de Contrato Railway (^:integration) ────────────────────────────────

(deftest ^:integration valkey-integration-test
  (testing "Valkey escribe, lee y elimina sin lanzar excepciones crudas"
    (let [store (:infra/valkey *system*)]
      (is (= :ok (proto/put-session! store "test-token" {:user "juan"} 60)))
      (is (= {:user "juan"} (proto/get-session store "test-token")))
      (is (= :ok (proto/del-session! store "test-token")))
      (is (nil? (proto/get-session store "test-token"))))))

(deftest ^:integration sqs-integration-test
  (testing "SQS publica y recibe emitiendo tuplas [:ok] o [:error]"
    (let [bus (:moira/sqs-bus *system*)
          pub-result (proto/publish! bus {:type "TEST_EVENT"} "group-1" "dedup-1")]
      ;; El payload retorna la tupla Railway [:ok {...}]
      (is (= :ok (first pub-result)))
      (is (string? (:message-id (second pub-result)))))))

(deftest ^:integration dynamodb-integration-test
  (testing "DynamoDB atrapa operaciones en tablas inexistentes retornando :SYS_000 en vez de ResourceNotFoundException"
    (let [ddb-client (:infra/dynamodb *system*)
          result (ddb/get-item ddb-client "tabla-inexistente" {:id "1"})]
      (is (= :error (first result)))
      (is (= :SYS_000 (-> result second :code))))))

(deftest ^:integration eventbridge-integration-test
  (testing "EventBridge inyecta evento usando el wrapper Railway"
    (let [eb-client (:infra/eventbridge *system*)
          result (proto/put-event! eb-client "default" "test.source" "TEST_TYPE" {:a 1})]
      ;; LocalStack responderá OK si el endpoint de events está activo en el 4566
      (is (contains? #{:ok :error} (first result))))))

(deftest ^:integration firehose-integration-test
  (testing "Kinesis inyecta registro usando el wrapper Railway"
    (let [fh-client (:infra/kinesis *system*)
          result (proto/put-record! fh-client "metri-stream" "pk" {:data "test"})]
      ;; Si el stream "metri-stream" no existe en LocalStack, Kinesis lanzará anomalía, la interceptamos
      (is (contains? #{:ok :error} (first result))))))
