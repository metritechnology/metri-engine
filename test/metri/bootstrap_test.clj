(ns metri.bootstrap-test
  (:require [clojure.test :refer :all]
            [metri.bootstrap :as bootstrap]
            [metri.domain.errors :as errors]
            [metri.otel.spans :as otel]
            [metri.codice.stub :as codice]
            [metri.grpc.translator :as translator]
            [metri.infrastructure.datahike :as datahike]
            [metri.infrastructure.tenant-guard :as tenant-guard]))

(defn setup []
  (reset! @#'errors/catalog {:SYS_000 {:http-status 500 :grpc-status 13}}))

(use-fixtures :each (fn [f] (setup) (f)))

(deftest bootstrap-success-test
  (testing "BST-01 Todos los pasos OK"
    (with-redefs [bootstrap/exit! (fn [_] (throw (ex-info "Should not exit" {})))
                  datahike.api/database-exists? (constantly true)
                  datahike.api/connect (constantly {:dummy :conn})
                  datahike/transact-schema! (constantly :ok)
                  datahike.api/release (constantly :ok)
                  translator/init-grpc-status-map! (constantly :ok)
                  otel/init! (constantly :ok)
                  otel/set-attributes! (constantly :ok)
                  tenant-guard/ensure-tenant-schema! (constantly :ok)]
      (is (nil? (bootstrap/run-fail-fast!))))))

(deftest bootstrap-failures-test
  (testing "BST-02 errors/load-catalog! falla"
    (let [exit-code (atom nil)]
      (with-redefs [bootstrap/exit! (fn [code] (reset! exit-code code) (throw (ex-info "Exit" {})))
                    clojure.core/deref (fn [x] (if (= x #'errors/catalog) {} (deref x)))]
        (is (thrown? Exception (bootstrap/run-fail-fast!)))
        (is (= 1 @exit-code)))))

  (testing "BST-03 otel/init! falla"
    (let [exit-code (atom nil)]
      (with-redefs [bootstrap/exit! (fn [code] (reset! exit-code code) (throw (ex-info "Exit" {})))
                    otel/init! (fn [] (throw (ex-info "OTel fail" {})))
                    datahike.api/database-exists? (constantly true)
                    datahike.api/connect (constantly {:dummy :conn})
                    datahike/transact-schema! (constantly :ok)
                    datahike.api/release (constantly :ok)
                    translator/init-grpc-status-map! (constantly :ok)]
        (is (thrown? Exception (bootstrap/run-fail-fast!)))
        (is (= 1 @exit-code)))))

  (testing "BST-04 codice/load-schemas! falla"
    (let [exit-code (atom nil)]
      (with-redefs [bootstrap/exit! (fn [code] (reset! exit-code code) (throw (ex-info "Exit" {})))
                    otel/init! (constantly :ok)
                    otel/set-attributes! (fn [_ _] (throw (ex-info "Codice fail" {})))
                    datahike.api/database-exists? (constantly true)
                    datahike.api/connect (constantly {:dummy :conn})
                    datahike/transact-schema! (constantly :ok)
                    datahike.api/release (constantly :ok)
                    translator/init-grpc-status-map! (constantly :ok)]
        (is (thrown? Exception (bootstrap/run-fail-fast!)))
        (is (= 1 @exit-code)))))

  (testing "BST-07 datahike/audit-schema falla"
    (let [exit-code (atom nil)]
      (with-redefs [bootstrap/exit! (fn [code] (reset! exit-code code) (throw (ex-info "Exit" {})))
                    datahike.api/database-exists? (constantly true)
                    datahike.api/connect (fn [_] (throw (ex-info "DB fail" {})))]
        (is (thrown? Exception (bootstrap/run-fail-fast!)))
        (is (= 1 @exit-code)))))

  (testing "BST-10 tenant-guard/ensure-tenant-schema! falla si :tenant/id no existe"
    (let [exit-code (atom nil)]
      (with-redefs [bootstrap/exit! (fn [code] (reset! exit-code code) (throw (ex-info "Exit" {})))
                    datahike.api/database-exists? (constantly true)
                    datahike.api/connect (constantly {:dummy :conn})
                    datahike/transact-schema! (constantly :ok)
                    datahike.api/release (constantly :ok)
                    translator/init-grpc-status-map! (constantly :ok)
                    otel/init! (constantly :ok)
                    otel/set-attributes! (constantly :ok)
                    tenant-guard/ensure-tenant-schema! (fn [_] (throw (ex-info "Tenant Guard Fail" {})))]
        (is (thrown? Exception (bootstrap/run-fail-fast!)))
        (is (= 1 @exit-code))))))

(deftest bootstrap-sequence-test
  (testing "BST-06 Pasos posteriores no se ejecutan si anterior falla"
    (let [exit-code (atom nil)
          otel-called (atom false)]
      (with-redefs [bootstrap/exit! (fn [code] (reset! exit-code code) (throw (ex-info "Exit" {})))
                    datahike.api/database-exists? (constantly true)
                    datahike.api/connect (fn [_] (throw (ex-info "DB fail" {})))
                    otel/init! (fn [] (reset! otel-called true))]
        (is (thrown? Exception (bootstrap/run-fail-fast!)))
        (is (= 1 @exit-code))
        (is (false? @otel-called)))))
  
  (testing "BST-08 translator/init-grpc-status-map! ejecuta después de catalog"
    (let [map-called (atom false)]
      (with-redefs [bootstrap/exit! (fn [_] (throw (ex-info "Should not exit" {})))
                    datahike.api/database-exists? (constantly true)
                    datahike.api/connect (constantly {:dummy :conn})
                    datahike/transact-schema! (constantly :ok)
                    datahike.api/release (constantly :ok)
                    translator/init-grpc-status-map! (fn [c] (reset! map-called (not (empty? c))))
                    otel/init! (constantly :ok)
                    otel/set-attributes! (constantly :ok)
                    tenant-guard/ensure-tenant-schema! (constantly :ok)]
        (bootstrap/run-fail-fast!)
        (is @map-called)))))

(deftest tenant-guard-schema-test
  (testing "BST-09 tenant-guard/ensure-tenant-schema! verifica :tenant/id"
    (let [schema-called (atom nil)]
      (with-redefs [datahike/transact-schema! (fn [_ schema] (reset! schema-called schema))]
        (tenant-guard/ensure-tenant-schema! {:dummy :conn})
        (is (= :tenant/id (:db/ident (first @schema-called))))))))
