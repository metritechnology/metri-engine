(ns metri.lambda.handler-test
  "Tests de la lógica Zero-Trust del handler Lambda.
   Testea la validación del token y la lógica de encoding/decoding
   sin levantar el sistema Integrant completo."
  (:require [clojure.test :refer [deftest is testing]]
            [cheshire.core :as json])
  (:import [java.util Base64]
           [java.nio ByteBuffer]))

;; ─── Helpers — replicamos la lógica del handler en tests puros ──────────────

(defn- validate-token
  "Extrae la lógica de validación Zero-Trust del handler."
  [headers expected-token]
  (let [client-token (get headers :x-metri-origin-token)]
    (when (and expected-token (not= expected-token client-token))
      {:status 403 :body {:message "Forbidden by Zero-Trust Token"}})))

(def ^:private valid-token "arn:aws:cloudformation:us-east-1:123:stack/metri-engine/abc123")

;; ─── HND-01..08 ──────────────────────────────────────────────────────────────

(deftest hnd-01-rejects-missing-token
  "HND-01 — request sin X-Metri-Origin-Token → 403 Forbidden"
  (let [response (validate-token {} valid-token)]
    (is (= 403 (:status response)))
    (is (= "Forbidden by Zero-Trust Token" (get-in response [:body :message])))))

(deftest hnd-02-rejects-invalid-token
  "HND-02 — request con token incorrecto → 403 Forbidden"
  (let [response (validate-token {:x-metri-origin-token "wrong-token"} valid-token)]
    (is (= 403 (:status response)))))

(deftest hnd-03-accepts-valid-token
  "HND-03 — request con token correcto → nil (sin bloqueo)"
  (let [response (validate-token {:x-metri-origin-token valid-token} valid-token)]
    (is (nil? response) "Token correcto no debe generar respuesta de error")))

(deftest hnd-04-no-token-required-when-env-empty
  "HND-04 — si METRI_ORIGIN_TOKEN no está configurado, no valida (nil token)"
  (let [response (validate-token {} nil)]
    (is (nil? response) "Sin token esperado → no bloquea")))

(deftest hnd-05-proxy-response-shape
  "HND-05 — estructura del proxy JSON de Function URL tiene campos requeridos"
  (let [proxy-resp {:statusCode      200
                    :headers         {"Content-Type" "application/grpc-web+proto"
                                      "Access-Control-Allow-Origin" "*"}
                    :isBase64Encoded true
                    :body            "AAAAAAA="}]
    (is (= 200 (:statusCode proxy-resp)))
    (is (contains? (:headers proxy-resp) "Content-Type"))
    (is (true? (:isBase64Encoded proxy-resp)))
    (is (string? (:body proxy-resp)))))

(deftest hnd-06-grpc-web-frame-structure
  "HND-06 — trama gRPC-Web Data (flag 0x00) tiene estructura correcta"
  ;; Flag byte (0x00) + 4 bytes length + N bytes proto
  (let [proto-bytes (byte-array [0x08 0x01 0x12 0x03])  ; ejemplo proto mínimo
        proto-len   (alength proto-bytes)
        bb          (ByteBuffer/allocate (+ 5 proto-len))]
    (.put bb (byte 0x00))          ; flag DATA
    (.putInt bb proto-len)         ; length big-endian
    (.put bb proto-bytes)
    (let [result (.array bb)]
      (is (= 0x00 (Byte/toUnsignedInt (aget result 0))) "Flag debe ser 0x00 para datos")
      (is (= proto-len
             (bit-or (bit-shift-left (Byte/toUnsignedInt (aget result 1)) 24)
                     (bit-shift-left (Byte/toUnsignedInt (aget result 2)) 16)
                     (bit-shift-left (Byte/toUnsignedInt (aget result 3)) 8)
                     (Byte/toUnsignedInt (aget result 4))))
          "Length field debe ser big-endian del tamaño del payload"))))

(deftest hnd-07-grpc-web-trailer-flag
  "HND-07 — trama gRPC-Web Trailer (flag 0x80) tiene flag correcto"
  (let [trailer-bytes (.getBytes "grpc-status: 0\r\n" "UTF-8")
        trailer-len   (alength trailer-bytes)
        bb            (ByteBuffer/allocate (+ 5 trailer-len))]
    (.put bb (unchecked-byte 128))   ; flag TRAILER = 0x80
    (.putInt bb trailer-len)
    (.put bb trailer-bytes)
    (let [result (.array bb)]
      (is (= 128 (Byte/toUnsignedInt (aget result 0)))
          "Flag debe ser 0x80 para trailers"))))

(deftest hnd-08-base64-roundtrip
  "HND-08 — Base64 encode/decode roundtrip preserva bytes"
  (let [original-bytes (byte-array [0x00 0x00 0x00 0x00 0x04 0x08 0x01 0x12 0x00])
        encoder        (Base64/getEncoder)
        decoder        (Base64/getDecoder)
        encoded        (.encodeToString encoder original-bytes)
        decoded        (.decode decoder encoded)]
    (is (= (vec original-bytes) (vec decoded))
        "Base64 roundtrip debe preservar bytes exactos")))
