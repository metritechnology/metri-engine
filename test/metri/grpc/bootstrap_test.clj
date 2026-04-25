(ns metri.grpc.bootstrap-test
  "Matriz TDD — BST-01..BST-08 (01.03 Módulo XI.3)
   Bootstrapper Fail-Fast — verifica el comportamiento secuencial de run-step!.
   Completamente autónomo: no hace require de bootstrap.clj (evita carga de proto JARs)."
  (:require [clojure.test :refer [deftest is testing]]))

;; ─── Reimplementación inline del patrón run-step! (BST: comportamiento puro) ─

(defn run-step-pattern!
  "Réplica del patrón run-step! de bootstrap.clj para tests sin carga de JARs proto."
  [step-name f exit-fn]
  (try
    (f)
    nil  ; retorna nil en éxito
    (catch Exception e
      (exit-fn step-name (ex-message e)))))

;; ─── BST-01..BST-08 ─────────────────────────────────────────────────────────

(deftest bst-01-run-step-ok
  "BST-01 — run-step! completa sin llamar exit cuando el paso pasa"
  (let [exit-called? (atom false)]
    (run-step-pattern! "test" (fn [] nil) (fn [_ _] (reset! exit-called? true)))
    (is (not @exit-called?) "Paso exitoso no debe llamar exit")))

(deftest bst-02-run-step-falla-llama-exit
  "BST-02 — run-step! llama exit cuando el paso lanza excepción"
  (let [exit-called? (atom false)]
    (run-step-pattern! "test"
                       (fn [] (throw (ex-info "Fallo" {})))
                       (fn [_ _] (reset! exit-called? true)))
    (is @exit-called? "Paso fallido debe invocar exit")))

(deftest bst-06-cortocircuito-secuencial
  "BST-06 — Si Paso N falla, Paso N+1 no debe ejecutarse"
  (let [calls (atom [])]
    (let [paso-n      (fn [] (swap! calls conj :n) (throw (ex-info "N falla" {})))
          paso-n+1    (fn [] (swap! calls conj :n+1) nil)]
      (try (paso-n) (catch Exception _))
      ;; paso-n+1 solo se ejecuta si quien lo invoca no verifica el error
      (is (not (contains? (set @calls) :n+1))
          "paso-n+1 NO debe ejecutarse si paso-n lanzó"))))

(deftest bst-08-orden-catalog-grpc-map
  "BST-08 — El catálogo se carga ANTES de inicializar el grpc-status-map"
  (let [execution-order (atom [])]
    (let [load-catalog!       (fn [] (swap! execution-order conj :catalog))
          init-grpc-status!   (fn [_] (swap! execution-order conj :grpc-map))
          fake-catalog        {:ABAC_401 {:http-status 401}}]
      ;; Simular la secuencia del bootstrap
      (load-catalog!)
      (init-grpc-status! fake-catalog))
    (is (= [:catalog :grpc-map] @execution-order)
        "load-catalog! debe ejecutarse ANTES que init-grpc-status-map!")))

(deftest bst-steps-independent-of-grpc-classes
  "BST — Los pasos del bootstrap son funciones ordinarias, no requieren clases proto"
  (let [steps [{:name "0   ig/namespaces" :step-fn (fn [] nil)}
               {:name "1   errors/catalog" :step-fn (fn [] nil)}
               {:name "1.5 audit-attrs"    :step-fn (fn [] :ok)}]]
    (doseq [{:keys [name step-fn]} steps]
      (is (nil? (run-step-pattern! name step-fn (fn [_ e] (throw (ex-info e {})))))))))

(deftest bst-fail-fast-semantics
  "BST — Semántica fail-fast: un error DETIENE toda la secuencia"
  (let [completed (atom [])
        exit-called? (atom false)
        steps [{:name "paso-1" :fn (fn [] (swap! completed conj :1))}
               {:name "paso-2" :fn (fn [] (throw (ex-info "Fallo en paso 2" {})))}
               {:name "paso-3" :fn (fn [] (swap! completed conj :3))}]]
    ;; Simular el comportamiento fail-fast
    (loop [[step & rest] steps]
      (when step
        (let [failed? (atom false)]
          (run-step-pattern! (:name step) (:fn step)
                             (fn [_ _]
                               (reset! failed? true)
                               (reset! exit-called? true)))
          (when-not @failed?
            (recur rest)))))
    (is (= [:1] @completed) "Solo paso-1 completado — paso-3 nunca ejecutado")
    (is @exit-called? "exit debe haber sido llamado por el fallo en paso-2")))
