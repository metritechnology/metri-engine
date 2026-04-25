(ns metri.iop.sherlog-test
  "Matriz TDD — SHL-01..08 (IOP Módulo V)
   Testea el despachador Sherlog: IFaultNotifier y process-fault!
   100% unitario — sin I/O, sin EventBridge real.

   Firmas:
     (notify! notifier error-dto severity) → :ok | :error
     (process-fault! notifier error-map error-dto) → side-effectful, retorna nil"
  (:require [clojure.test :refer [deftest is testing]]
            [metri.iop.sherlog :as sherlog]
            [metri.domain.errors :as errors]))

;; ─── Stub canónico — InMemoryFaultNotifier ───────────────────────────────────

(defrecord InMemoryFaultNotifier [calls-atom]
  sherlog/IFaultNotifier
  (notify! [_ error-dto severity]
    (swap! calls-atom conj {:dto error-dto :severity severity})
    :ok))

(defrecord FailingFaultNotifier []
  sherlog/IFaultNotifier
  (notify! [_ _ _] :error))

(defn make-stub  [] (->InMemoryFaultNotifier (atom [])))
(defn make-fail  [] (->FailingFaultNotifier))

;; ─── SHL-01..08 ──────────────────────────────────────────────────────────────

(deftest shl-01-notify-ok-retorna-ok
  "SHL-01 — notify! happy path retorna :ok"
  (let [n (make-stub)]
    (is (= :ok (sherlog/notify! n {:code :ABAC_401} :error)))))

(deftest shl-02-notify-registra-llamada
  "SHL-02 — notify! acumula la llamada en el stub"
  (let [n (make-stub)]
    (sherlog/notify! n {:code :ABAC_401} :error)
    (is (= 1 (count @(:calls-atom n))))
    (is (= :error (:severity (first @(:calls-atom n)))))))

(deftest shl-03-notify-failing-retorna-error
  "SHL-03 — notify! que falla retorna :error (Railway)"
  (let [n (make-fail)]
    (is (= :error (sherlog/notify! n {:code :ABAC_401} :fatal)))))

(deftest shl-04-satisfies-protocol
  "SHL-04 — Liskov: InMemoryFaultNotifier satisfies IFaultNotifier"
  (is (satisfies? sherlog/IFaultNotifier (make-stub))))

(deftest shl-05-process-fault-warning-invoca-notify
  "SHL-05 — process-fault! con severidad :warning invoca notify!"
  (let [n       (make-stub)
        ;; Usamos un lookup-fn que simula una entrada de catálogo :warning
        lookup  (fn [_] {:severity :warning :retryable? true})
        err-map {:code :ABAC_401}
        err-dto {:code "ABAC_401" :detail "test"}]
    ;; Invocamos directamente con lookup inyectado vía redefinición
    (with-redefs [errors/lookup lookup]
      (sherlog/process-fault! n err-map err-dto))
    (is (= 1 (count @(:calls-atom n)))
        "notify! debe invocarse para severidad :warning")))

(deftest shl-06-process-fault-info-no-invoca-notify
  "SHL-06 — process-fault! con severidad :info NO invoca notify!"
  (let [n      (make-stub)
        lookup (fn [_] {:severity :info})]
    (with-redefs [errors/lookup lookup]
      (sherlog/process-fault! n {:code :SYS_INFO_001} {:code "SYS_INFO_001"}))
    (is (zero? (count @(:calls-atom n)))
        "notify! NO debe invocarse para severidad :info")))

(deftest shl-07-process-fault-fatal-invoca-notify
  "SHL-07 — process-fault! con severidad :fatal invoca notify! con :fatal"
  (let [n      (make-stub)
        lookup (fn [_] {:severity :fatal :retryable? false})]
    (with-redefs [errors/lookup lookup]
      (sherlog/process-fault! n {:code :SYS_FATAL} {:code "SYS_FATAL"}))
    (is (= :fatal (:severity (first @(:calls-atom n)))))
    (is (= 1 (count @(:calls-atom n))))))

(deftest shl-08-process-fault-codigo-desconocido-no-lanza
  "SHL-08 — process-fault! con código desconocido (lookup nil) no lanza excepción"
  (let [n      (make-stub)
        lookup (fn [_] nil)] ;; catálogo vacío
    (is (= :ok
          (with-redefs [errors/lookup lookup]
            (sherlog/process-fault! n {:code :CODIGO_DESCONOCIDO} {})))
        "process-fault! nunca debe lanzar, incluso con código no registrado")))
