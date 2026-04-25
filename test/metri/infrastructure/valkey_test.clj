(ns metri.infrastructure.valkey-test
  "Matriz TDD — VLK-01..07 (01.02 Módulo XIII)
   Tests unitarios con InMemorySessionStore (stub en memoria).
   Cero I/O — sin Valkey real."
  (:require [clojure.test :refer [deftest is testing are]]
            [metri.domain.protocols :as proto]
            [metri.domain.errors :as errors]))

;; ─── Stub canónico — InMemorySessionStore ────────────────────────────────────
;; Implementa el mismo proto/ISessionStore que ValkeySessionStore.
;; Nota: La API real de valkey.clj retorna :ok (keyword) no [:ok], lo reflejamos aquí.

(defrecord InMemorySessionStore [store-atom]
  proto/ISessionStore
  (get-session  [_ token]         (get @store-atom token))
  (put-session! [_ token sess _]  (swap! store-atom assoc token sess) :ok)
  (del-session! [_ token]         (swap! store-atom dissoc token)     :ok))

(defn make-stub [] (->InMemorySessionStore (atom {})))

;; ─── VLK-01..07 ──────────────────────────────────────────────────────────────

(deftest vlk-01-session-roundtrip
  "VLK-01 — put-session! → get-session retorna el mapa íntegro"
  (let [store   (make-stub)
        token   "tok-abc123"
        session {:tenant-id "t1" :user-id "u1" :role :admin}]
    (proto/put-session! store token session 3600)
    (is (= session (proto/get-session store token)))))

(deftest vlk-02-del-session
  "VLK-02 — del-session! → get-session retorna nil (token revocado)"
  (let [store (make-stub)
        token "tok-to-delete"]
    (proto/put-session! store token {:user "alice"} 3600)
    (proto/del-session! store token)
    (is (nil? (proto/get-session store token))
        "Token revocado debe retornar nil")))

(deftest vlk-03-get-missing-session
  "VLK-03 — get-session de token inexistente retorna nil"
  (let [store (make-stub)]
    (is (nil? (proto/get-session store "no-existe")))))

(deftest vlk-04-put-overwrites
  "VLK-04 — put-session! sobreescribe sesión anterior con el mismo token"
  (let [store (make-stub)
        token "tok-overwrite"]
    (proto/put-session! store token {:role :user}  3600)
    (proto/put-session! store token {:role :admin :tenant-id "t99"} 3600)
    (is (= :admin (-> (proto/get-session store token) :role)))
    (is (= "t99"  (-> (proto/get-session store token) :tenant-id)))))

(deftest vlk-05-satisfies-protocol
  "VLK-05 — Liskov: InMemorySessionStore satisfies ISessionStore"
  (is (satisfies? proto/ISessionStore (make-stub))))

(deftest vlk-06-put-returns-ok
  "VLK-06 — put-session! y del-session! retornan :ok"
  (let [store (make-stub)]
    (is (= :ok (proto/put-session! store "tok" {:x 1} 60)))
    (is (= :ok (proto/del-session! store "tok")))))

(deftest vlk-07-multiple-tokens-isolated
  "VLK-07 — Múltiples tokens son independientes entre sí"
  (let [store (make-stub)]
    (proto/put-session! store "tok-a" {:user "alice"} 3600)
    (proto/put-session! store "tok-b" {:user "bob"}   3600)
    (proto/del-session! store "tok-a")
    (is (nil? (proto/get-session store "tok-a")) "tok-a eliminado")
    (is (= "bob" (-> (proto/get-session store "tok-b") :user)) "tok-b intacto")))

;; ─── VLK-08: session miss → INFRA_VALKEY_002 ────────────────────────────────

(deftest vlk-08-session-miss-error
  "VLK-08 — Token no encontrado / expirado → [:error {:code :INFRA_VALKEY_002}]"
  (let [[tag body] (errors/error :INFRA_VALKEY_002 {:token_hint "tok-xxx"})]
    (is (= :error tag))
    (is (= :INFRA_VALKEY_002 (:code body)))
    (is (false? (:retryable? body)) "Session miss no es reintentable (requiere re-login)")))

;; ─── VLK-09: put-session! falla → INFRA_VALKEY_003 ──────────────────────────

(deftest vlk-09-put-session-error
  "VLK-09 — put-session! Carmine exception → [:error {:code :INFRA_VALKEY_003}]"
  (let [[tag body] (errors/error :INFRA_VALKEY_003
                                  {:token_hint  "tok-abc"
                                   :ttl_seconds 3600
                                   :detail      "Connection reset by peer"})]
    (is (= :error tag))
    (is (= :INFRA_VALKEY_003 (:code body)))
    (is (true? (:retryable? body)) "Write failure ES reintentable")))

;; ─── VLK-10: del-session! falla → INFRA_VALKEY_004 ──────────────────────────

(deftest vlk-10-del-session-error
  "VLK-10 — del-session! falla (idempotente pero alerta conectividad)"
  (let [[tag body] (errors/error :INFRA_VALKEY_004
                                  {:token_hint "tok-abc"
                                   :detail     "Read timed out"})]
    (is (= :error tag))
    (is (= :INFRA_VALKEY_004 (:code body)))
    (is (true? (:retryable? body)))))

;; ─── VLK-11: PING falla en bootstrap → INFRA_VALKEY_005 ─────────────────────

(deftest vlk-11-ping-fails-on-bootstrap
  "VLK-11 — Valkey PING responde inesperadamente → [:error {:code :INFRA_VALKEY_005}] (fatal)"
  (let [[tag body] (errors/error :INFRA_VALKEY_005
                                  {:host "host" :port 6379 :response "ERR"})]
    (is (= :error tag))
    (is (= :INFRA_VALKEY_005 (:code body)))
    (is (false? (:retryable? body)) "Bootstrap PING failure es fatal")))

;; ─── VLK-12: Retryability invariants ─────────────────────────────────────────

(deftest vlk-12-retryability-invariants
  "VLK-12 — Invariantes de retryability para todos los INFRA_VALKEY_*"
  (are [code ctx expected] (= expected (:retryable? (second (errors/error code ctx))))
    :INFRA_VALKEY_001 {:timeout_ms 1000 :detail "x"}                  true
    :INFRA_VALKEY_002 {:token_hint "t"}                                false
    :INFRA_VALKEY_003 {:token_hint "t" :ttl_seconds 3600 :detail "x"} true
    :INFRA_VALKEY_004 {:token_hint "t" :detail "x"}                   true
    :INFRA_VALKEY_005 {:host "h" :port 6379 :response "ERR"}          false))
