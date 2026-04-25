(ns metri.iop.pipeline-test
  "Tests del motor Railway del IOP Pipeline — chain/run (puro, sin stubs).
   Ningún test hace I/O — son funciones puras del motor de composición.
   9 tests cubriendo el contrato completo del Railway engine."
  (:require [clojure.test :refer [deftest is testing are]]
            [metri.iop.pipeline :as pipeline]))

;; ─── Helpers: pasos del pipeline como fns puras ─────────────────────────────

(defn step-ok
  "Paso que retorna [:ok ctx-enriquecido]."
  [label]
  (fn [ctx] [:ok (assoc ctx :steps (conj (get ctx :steps []) label))]))

(defn step-error
  "Paso que retorna [:error {:code :TEST_ERR}]."
  [label]
  (fn [_ctx] [:error {:code :TEST_ERR :step label}]))

(defn step-throws
  "Paso que lanza una excepción."
  [label]
  (fn [_ctx] (throw (ex-info (str "Exception in " label) {:step label}))))

;; ─── Tests del motor chain/run ──────────────────────────────────────────────

(deftest pipeline-chain-todos-ok
  "IOP-01 — chain con todos los pasos OK retorna [:ok ctx-enriquecido]"
  (let [steps  [(step-ok :cedar) (step-ok :quota) (step-ok :janus)]
        ctx    {:request {:entity-type "asset"}}
        result (pipeline/run steps ctx)]
    (is (= :ok (first result)))
    (is (= [:cedar :quota :janus] (:steps (second result))))))

(deftest pipeline-chain-cortocircuita-en-error
  "IOP-02 — chain cortocircuita en el primer error (Railway short-circuit)"
  (let [pasos-llamados (atom [])
        step-spy       (fn [label]
                         (fn [ctx]
                           (swap! pasos-llamados conj label)
                           [:ok ctx]))
        step-err       (fn [label]
                         (fn [ctx]
                           (swap! pasos-llamados conj label)
                           [:error {:code :TEST_ERR}]))
        steps  [(step-spy  :paso-1)
                (step-err  :paso-2)
                (step-spy  :paso-3)]   ; este NO debe ejecutarse
        ctx    {:request {}}
        result (pipeline/run steps ctx)]
    (is (= :error (first result)))
    (is (= [:paso-1 :paso-2] @pasos-llamados)
        "paso-3 NO debe ejecutarse tras error en paso-2")))

(deftest pipeline-chain-un-solo-paso
  "IOP-03 — chain con un único paso funciona"
  (let [result (pipeline/run [(step-ok :solo)] {:request {}})]
    (is (= :ok (first result)))
    (is (= [:solo] (:steps (second result))))))

(deftest pipeline-chain-sin-pasos
  "IOP-04 — chain sin pasos retorna el ctx original como [:ok ctx]"
  (let [ctx    {:request {:entity-type "test"}}
        result (pipeline/run [] ctx)]
    (is (= :ok   (first result)))
    (is (= ctx   (second result)))))

(deftest pipeline-primer-paso-error
  "IOP-05 — si el primer paso falla, ningún paso adicional se ejecuta"
  (let [segunda-fn-llamada? (atom false)
        steps [(step-error :first-fail)
               (fn [ctx] (reset! segunda-fn-llamada? true) [:ok ctx])]
        result (pipeline/run steps {:request {}})]
    (is (= :error (first result)))
    (is (not @segunda-fn-llamada?) "Segunda fn no debe ejecutarse")))

(deftest pipeline-ctx-se-acumula-entre-pasos
  "IOP-06 — el ctx se pasa y acumula entre pasos correctamente"
  (let [paso-add-a (fn [ctx] [:ok (assoc ctx :a 1)])
        paso-add-b (fn [ctx] [:ok (assoc ctx :b 2)])
        paso-add-c (fn [ctx] [:ok (assoc ctx :c (+ (:a ctx) (:b ctx)))])]
    (let [[tag final-ctx] (pipeline/run [paso-add-a paso-add-b paso-add-c] {})]
      (is (= :ok tag))
      (is (= 1 (:a final-ctx)))
      (is (= 2 (:b final-ctx)))
      (is (= 3 (:c final-ctx)) "paso-c debe ver los valores de paso-a y paso-b"))))

(deftest pipeline-error-preserva-codigo
  "IOP-07 — el código de error Railway se preserva a través del chain"
  (let [steps  [(step-ok :cedar) (step-error :quota) (step-ok :janus)]
        [tag body] (pipeline/run steps {:request {}})]
    (is (= :error tag))
    (is (= :TEST_ERR (:code body)))
    (is (= :quota    (:step body)))))

(deftest pipeline-step-receives-ctx-from-previous
  "IOP-08 — Step 2 recibe el ctx enriquecido por Step 1, NO el ctx inicial."
  (let [captured-ctx (atom nil)
        paso-1       (fn [ctx] [:ok (assoc ctx :injected-by-step1 "yes")])
        paso-2       (fn [ctx]
                       (reset! captured-ctx ctx)
                       [:ok ctx])]
    (pipeline/run [paso-1 paso-2] {:initial true})
    (is (= "yes" (:injected-by-step1 @captured-ctx))
        "Step 2 debe recibir el ctx con :injected-by-step1 añadido por Step 1")
    (is (true? (:initial @captured-ctx))
        "Step 2 debe preservar el ctx inicial también")))

(deftest pipeline-error-body-preserved-through-chain
  "IOP-09 — El body completo del [:error] se preserva intacto hasta el final."
  (let [rich-error {:stage :janus :code :JNS_VAL_001 :detail "Schema violation"
                    :tenant-id "tnt-x" :user-id "usr-y"}
        steps [(step-ok :cedar)
               (fn [_ctx] [:error rich-error])
               (step-ok :janus)]
        [tag body] (pipeline/run steps {:request {}})]
    (is (= :error tag))
    (is (= rich-error body)
        "El body del [:error] debe llegar intacto al caller del pipeline")))
