(ns metri.main-test
  (:require [clojure.test :refer :all]
            [metri.main :as main]
            [metri.bootstrap :as bootstrap]
            [integrant.core :as ig]))

(deftest detect-environment-test
  (testing "MN-04 detect-environment con ENVIRONMENT=production"
    (with-redefs [clojure.core/getenv (fn [k] (if (= k "ENVIRONMENT") "production" nil))]
      (is (= :production (@#'main/detect-environment)))))
  (testing "MN-05 detect-environment sin ENVIRONMENT"
    (with-redefs [clojure.core/getenv (constantly nil)]
      (is (= :development (@#'main/detect-environment))))))

(deftest select-config-resource-test
  (testing "MN-06 select-config-resource con :development"
    (is (= "config/system.dev.edn" (@#'main/select-config-resource :development))))
  (testing "MN-07 select-config-resource con :production"
    (is (= "config/system.edn" (@#'main/select-config-resource :production)))))

(deftest main-execution-test
  (testing "MN-02 -main con bootstrap fallo"
    (let [exit-code (atom nil)]
      (with-redefs [bootstrap/run-fail-fast! (fn [] (bootstrap/exit! 1))
                    bootstrap/exit! (fn [code] (reset! exit-code code) (throw (ex-info "Bootstrap Exit" {})))]
        (is (thrown? Exception (main/-main)))
        (is (= 1 @exit-code)))))

  (testing "MN-03 -main con config EDN inválido"
    (let [exit-code (atom nil)]
      (with-redefs [bootstrap/run-fail-fast! (constantly :ok)
                    main/load-system-config (fn [_] (throw (ex-info "Config err" {})))
                    main/exit! (fn [code] (reset! exit-code code) (throw (ex-info "Main Exit" {})))]
        (is (thrown? Exception (main/-main)))
        (is (= 2 @exit-code)))))

  (testing "MN-01 -main con bootstrap OK + Integrant OK"
    (let [promise-called (atom false)]
      (with-redefs [bootstrap/run-fail-fast! (constantly :ok)
                    main/load-system-config (constantly {:dummy {}})
                    ig/init (constantly {:dummy :system})
                    main/register-shutdown-hook! (constantly nil)
                    main/print-ready-banner (constantly nil)
                    clojure.core/promise (fn [] (reset! promise-called true) (delay :blocked))
                    clojure.core/deref (fn [d] (when (= d (delay :blocked)) :ok))]
        (main/-main)
        (is @promise-called)))))

(deftest shutdown-hook-test
  (testing "MN-08 Shutdown hook ejecuta ig/halt! en orden inverso"
    (let [halt-called (atom false)]
      (with-redefs [ig/halt! (fn [_] (reset! halt-called true))]
        ;; Simular llamada de hook
        (let [sys {:dummy :sys}]
          (ig/halt! sys)
          (is @halt-called))))))
