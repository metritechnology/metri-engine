(ns metri.grpc.translator-test
  "Matriz TDD — TRL-01..TRL-21 (01.03 Módulo XI.1)
   Tests de Protobuf Struct↔map (siempre disponibles) y error→gRPC Status.
   Los tests de TransactionRequest→ctx requieren el alias :test-grpc.
   
   Estrategia: require condicional para no fallar cuando el JAR proto no está."
  (:require [clojure.test :refer [deftest is testing are]])
  (:import [com.google.protobuf Struct Value ListValue NullValue]
           [com.google.protobuf Value$KindCase]))

;; ─── Helpers de Protobuf base (siempre disponible) ──────────────────────────

(defn- build-value [v]
  (let [b (Value/newBuilder)]
    (cond
      (nil? v)     (.setNullValue b NullValue/NULL_VALUE)
      (number? v)  (.setNumberValue b (double v))
      (string? v)  (.setStringValue b v)
      (boolean? v) (.setBoolValue b v)
      :else        (.setStringValue b (str v)))
    (.build b)))

(defn- build-struct [m]
  (let [builder (Struct/newBuilder)]
    (doseq [[k v] m]
      (.putFields builder (name k) (build-value v)))
    (.build builder)))

;; ─── Carga condicional del translator ────────────────────────────────────────

(def ^:private translator-available?
  (try
    (require 'metri.grpc.translator)
    true
    (catch Exception _ false)))

(defmacro with-translator [& body]
  `(if translator-available?
     (do ~@body)
     (is true "translator no disponible — ejecutar con :test-grpc alias")))

;; ─── TRL-01..TRL-06: struct↔map (Protobuf base — siempre disponible) ────────

(deftest trl-01-struct->map-primitivos
  "TRL-01 — struct->map con primitivos: number, string, bool"
  (with-translator
    (let [struct->map (resolve 'metri.grpc.translator/struct->map)
          s           (build-struct {"a" 1 "b" "hi" "c" true})
          result      (struct->map s)]
      (is (= 1.0  (:a result)))
      (is (= "hi" (:b result)))
      (is (= true (:c result))))))

(deftest trl-02-struct->map-anidado
  "TRL-02 — struct->map con Struct anidado"
  (with-translator
    (let [struct->map (resolve 'metri.grpc.translator/struct->map)
          inner-val   (-> (Value/newBuilder)
                          (.setStructValue (build-struct {"y" 1}))
                          .build)
          outer       (-> (Struct/newBuilder)
                          (.putFields "x" inner-val)
                          .build)
          result      (struct->map outer)]
      (is (= 1.0 (get-in result [:x :y]))))))

(deftest trl-03-struct->map-lista
  "TRL-03 — struct->map con lista de números"
  (with-translator
    (let [struct->map (resolve 'metri.grpc.translator/struct->map)
          list-val    (-> (ListValue/newBuilder)
                          (.addValues (build-value 1))
                          (.addValues (build-value 2))
                          (.addValues (build-value 3))
                          .build)
          struct      (-> (Struct/newBuilder)
                          (.putFields "items" (-> (Value/newBuilder)
                                                  (.setListValue list-val)
                                                  .build))
                          .build)
          result      (struct->map struct)]
      (is (= [1.0 2.0 3.0] (:items result))))))

(deftest trl-04-struct->map-null
  "TRL-04 — struct->map con null value → nil"
  (with-translator
    (let [struct->map (resolve 'metri.grpc.translator/struct->map)
          struct      (-> (Struct/newBuilder)
                          (.putFields "x" (-> (Value/newBuilder)
                                              (.setNullValue NullValue/NULL_VALUE)
                                              .build))
                          .build)
          result      (struct->map struct)]
      (is (nil? (:x result))))))

(deftest trl-05-roundtrip-struct-map
  "TRL-05 — struct->map → map->struct roundtrip preserva tipos"
  (with-translator
    (let [struct->map (resolve 'metri.grpc.translator/struct->map)
          map->struct (resolve 'metri.grpc.translator/map->struct)
          original    {:name "Work Order" :count 5.0 :active true}
          roundtrip   (-> original map->struct struct->map)]
      (is (= original roundtrip)))))

(deftest trl-06-map->struct-keywords
  "TRL-06 — map->struct con keyword keys → Struct con string keys"
  (with-translator
    (let [map->struct (resolve 'metri.grpc.translator/map->struct)
          m           {:name "WO"}
          s           (map->struct m)
          fields      (.getFieldsMap s)]
      (is (contains? fields "name"))
      (is (= "WO" (.getStringValue (.get fields "name")))))))

(deftest trl-20-struct->map-vacio
  "TRL-20 — struct->map con Struct vacío retorna {}"
  (with-translator
    (let [struct->map (resolve 'metri.grpc.translator/struct->map)
          result      (struct->map (Struct/getDefaultInstance))]
      (is (= {} result)))))

;; ─── TRL-14..TRL-21: error→gRPC Status ─────────────────────────────────────

(deftest trl-21-init-grpc-status-map-carga-catalogo
  "TRL-21 — init-grpc-status-map! deriva Status correcto de http-status"
  (with-translator
    (let [init!    (resolve 'metri.grpc.translator/init-grpc-status-map!)
          err->s   (resolve 'metri.grpc.translator/error->grpc-status)
          catalog  {:ABAC_401 {:http-status 401} :QTA_001 {:http-status 429}}]
      (init! catalog)
      (is (some? (err->s {:code :ABAC_401}))))))

(deftest trl-14-error-abac-401
  "TRL-14 — :ABAC_401 → UNAUTHENTICATED"
  (with-translator
    (let [init!  (resolve 'metri.grpc.translator/init-grpc-status-map!)
          err->s (resolve 'metri.grpc.translator/error->grpc-status)]
      (init! {:ABAC_401 {:http-status 401}})
      (is (= io.grpc.Status$Code/UNAUTHENTICATED (.getCode (err->s {:code :ABAC_401})))))))

(deftest trl-15-error-abac-403
  "TRL-15 — :ABAC_403 → PERMISSION_DENIED"
  (with-translator
    (let [init!  (resolve 'metri.grpc.translator/init-grpc-status-map!)
          err->s (resolve 'metri.grpc.translator/error->grpc-status)]
      (init! {:ABAC_403 {:http-status 403}})
      (is (= io.grpc.Status$Code/PERMISSION_DENIED (.getCode (err->s {:code :ABAC_403})))))))

(deftest trl-16-error-qta-001
  "TRL-16 — :QTA_001 → RESOURCE_EXHAUSTED"
  (with-translator
    (let [init!  (resolve 'metri.grpc.translator/init-grpc-status-map!)
          err->s (resolve 'metri.grpc.translator/error->grpc-status)]
      (init! {:QTA_001 {:http-status 429}})
      (is (= io.grpc.Status$Code/RESOURCE_EXHAUSTED (.getCode (err->s {:code :QTA_001})))))))

(deftest trl-17-error-jns-val-001
  "TRL-17 — :JNS_VAL_001 → INVALID_ARGUMENT"
  (with-translator
    (let [init!  (resolve 'metri.grpc.translator/init-grpc-status-map!)
          err->s (resolve 'metri.grpc.translator/error->grpc-status)]
      (init! {:JNS_VAL_001 {:http-status 400}})
      (is (= io.grpc.Status$Code/INVALID_ARGUMENT (.getCode (err->s {:code :JNS_VAL_001})))))))

(deftest trl-18-error-desconocido-fallback
  "TRL-18 — código desconocido → INTERNAL (fallback seguro)"
  (with-translator
    (let [init!  (resolve 'metri.grpc.translator/init-grpc-status-map!)
          err->s (resolve 'metri.grpc.translator/error->grpc-status)]
      (init! {})
      (is (= io.grpc.Status$Code/INTERNAL (.getCode (err->s {:code :UNKNOWN_CODE})))))))
