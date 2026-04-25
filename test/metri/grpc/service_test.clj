(ns metri.grpc.service-test
  "Matriz TDD — SVC-01..SVC-14 (01.03 Módulo XI.2)
   Testea el patrón handle-unary: translate → pipeline → response → observer.
   100% puro — sin imports de proto ni del namespace service (que requiere JARs gRPC).

   Los tests del MetriServiceImpl real requieren el alias :test-grpc con el JAR completo."
  (:require [clojure.test :refer [deftest is testing]])
  (:import [io.grpc.stub StreamObserver]
           [io.grpc Status StatusRuntimeException]))

;; ─── Reimplementación inline del patrón handle-unary (para tests puros) ─────
;; Replica fielmente la lógica de service.clj handle-unary — SRP: testamos el patrón.

(defn handle-unary-pattern
  "Réplica del patrón handle-unary para tests — NO importa el namespace real."
  [method-name ^StreamObserver observer translate-fn pipeline-fn response-fn request sherlog-fn]
  (try
    (let [ctx    (translate-fn request)
          result (pipeline-fn ctx)
          resp   (response-fn result)]
      (.onNext observer resp)
      (.onCompleted observer))
    (catch StatusRuntimeException e
      (.onError observer e))
    (catch Exception e
      ;; FASE 10 — excepción inesperada → Sherlog → onError
      (when sherlog-fn (sherlog-fn e))
      (.onError observer
        (-> (Status/INTERNAL)
            (.withDescription (ex-message e))
            (.withCause e)
            (.asRuntimeException))))))

;; ─── StreamObserver spy ──────────────────────────────────────────────────────

(defn make-observer []
  (let [responses  (atom [])
        errors     (atom [])
        completed? (atom false)]
    {:responses  responses
     :errors     errors
     :completed? completed?
     :observer
     (reify StreamObserver
       (onNext      [_ v] (swap! responses conj v))
       (onError     [_ e] (swap! errors   conj e))
       (onCompleted [_]   (reset! completed? true)))}))

;; ─── Pipeline stubs ──────────────────────────────────────────────────────────

(def translate-id     (fn [r] {:request r}))
(def response-to-str  (fn [[tag body]] (str tag "-" body)))
(defn pipeline-ok   [_] [:ok {:entity-id "stub-123"}])
(defn pipeline-err  [_] [:error {:code :ABAC_401 :message "No session"}])
(defn pipeline-boom [_] (throw (ex-info "Pipeline exploded" {:reason :test})))

;; ─── SVC-01..SVC-14 ─────────────────────────────────────────────────────────

(deftest svc-01-handle-unary-ok
  "SVC-01 — pipeline OK → observer.onNext (1 vez) + observer.onCompleted"
  (let [{:keys [observer responses completed?]} (make-observer)]
    (handle-unary-pattern "transact" observer translate-id pipeline-ok response-to-str :req nil)
    (is (= 1 (count @responses)) "onNext invocado exactamente 1 vez")
    (is @completed? "onCompleted invocado")))

(deftest svc-02-handle-unary-error-railway
  "SVC-02 — pipeline [:error] Railway → observer.onNext con error serializado (NO onError)"
  (let [{:keys [observer responses errors]} (make-observer)]
    (handle-unary-pattern "transact" observer translate-id pipeline-err response-to-str :req nil)
    (is (pos? (count @responses)) "Error Railway → onNext (respuesta de error serializada)")
    (is (= 0 (count @errors)) "Error Railway NO usa observer.onError")))

(deftest svc-03-handle-unary-exception-usa-onError
  "SVC-03 — pipeline lanza Exception → observer.onError con Status.INTERNAL"
  (let [{:keys [observer errors responses]} (make-observer)]
    (handle-unary-pattern "transact" observer translate-id pipeline-boom response-to-str :req nil)
    (is (pos? (count @errors)) "Exception → observer.onError")
    (is (= 0 (count @responses)) "Exception NO llama onNext")))

(deftest svc-10-handle-unary-nunca-lanza
  "SVC-10 — handle-unary absorbe TODA excepción — nunca propaga al caller (Netty)"
  (let [{:keys [observer]} (make-observer)]
    (is (nil? (handle-unary-pattern "transact" observer translate-id pipeline-boom response-to-str :req nil))
        "handle-unary NUNCA lanza — absorbe y despacha a observer.onError")))

(deftest svc-11-dip-pipeline-inyectable
  "SVC-11 — DIP: cambiar el stub del pipeline cambia el comportamiento"
  (let [{:keys [observer responses]} (make-observer)]
    (handle-unary-pattern "transact" observer translate-id pipeline-ok response-to-str :req nil)
    (is (.contains (first @responses) "ok") "Stub ok → respuesta contiene 'ok'"))
  (let [{:keys [observer responses]} (make-observer)]
    (handle-unary-pattern "transact" observer translate-id pipeline-err response-to-str :req nil)
    (is (.contains (first @responses) "error") "Stub error → respuesta contiene 'error'")))

(deftest svc-13-exception-invoca-sherlog
  "SVC-13 — Exception en pipeline → Sherlog es invocado con la excepción (FASE 10)"
  (let [{:keys [observer]} (make-observer)
        sherlog-calls (atom [])]
    (handle-unary-pattern "transact" observer translate-id pipeline-boom response-to-str
                          :req (fn [e] (swap! sherlog-calls conj e)))
    (is (= 1 (count @sherlog-calls)) "Sherlog invocado exactamente 1 vez por exception")))

(deftest svc-14-railway-error-no-invoca-sherlog
  "SVC-14 — Error Railway [:error] → Sherlog NO invocado (FASE 10 G10)"
  (let [{:keys [observer]} (make-observer)
        sherlog-calls (atom [])]
    (handle-unary-pattern "transact" observer translate-id pipeline-err response-to-str
                          :req (fn [e] (swap! sherlog-calls conj e)))
    (is (= 0 (count @sherlog-calls))
        "Sherlog NO debe invocarse por errors Railway controlados [:error]")))
