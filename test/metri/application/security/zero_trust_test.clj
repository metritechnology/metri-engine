(ns metri.application.security.zero-trust-test
  "Matriz TDD — ZT-01..08 (01.01 §2.2 Zero-Trust Perimetral)
   Testea la lógica de validación del header X-Metri-Origin-Token
   a nivel de aplicación — la misma lógica que vive en handler.clj.

   Estrategia: replica la lógica de validación como funciones puras testables
   sin instanciar el sistema Integrant completo.
   
   Fuente de verdad: template.yaml §2.2 — X-Metri-Origin-Token: !Sub ${AWS::StackId}"
  (:require [clojure.test :refer [deftest is testing are]]))

;; ─── Lógica de validación extraída (replica handler.clj) ─────────────────────

(defn- validate-zero-trust
  "Replica la lógica de validación Zero-Trust del handler Lambda.
   Retorna nil si el request es legítimo, o un mapa de error {:status 403} si es rechazado."
  [headers expected-token]
  (let [client-token (or (get headers "x-metri-origin-token")
                         (get headers :x-metri-origin-token))]
    (when (and expected-token (not= expected-token client-token))
      {:status 403 :body {:message "Forbidden by Zero-Trust Token"}})))

(defn- extract-token
  "Extrae el token Zero-Trust del mapa de headers (case-insensitive)."
  [headers]
  (or (get headers "x-metri-origin-token")
      (get headers :x-metri-origin-token)
      (get headers "X-Metri-Origin-Token")))

(def ^:private stack-id-example
  "arn:aws:cloudformation:us-east-1:982592308819:stack/metri-engine/abc-123-def")

;; ─── ZT-01..08 ───────────────────────────────────────────────────────────────

(deftest zt-01-token-correcto-permite-paso
  "ZT-01 — Header correcto → nil (request permitido)"
  (let [headers {"x-metri-origin-token" stack-id-example}
        result  (validate-zero-trust headers stack-id-example)]
    (is (nil? result) "Token correcto no debe bloquear")))

(deftest zt-02-token-ausente-bloquea
  "ZT-02 — Header ausente → 403 Forbidden"
  (let [result (validate-zero-trust {} stack-id-example)]
    (is (= 403 (:status result)))
    (is (= "Forbidden by Zero-Trust Token" (get-in result [:body :message])))))

(deftest zt-03-token-incorrecto-bloquea
  "ZT-03 — Header con valor incorrecto → 403 Forbidden"
  (let [headers {"x-metri-origin-token" "wrong-stack-id"}
        result  (validate-zero-trust headers stack-id-example)]
    (is (= 403 (:status result)))))

(deftest zt-04-token-vacio-bloquea
  "ZT-04 — Header presente pero vacío → 403 Forbidden"
  (let [headers {"x-metri-origin-token" ""}
        result  (validate-zero-trust headers stack-id-example)]
    (is (= 403 (:status result)))))

(deftest zt-05-sin-expected-token-no-valida
  "ZT-05 — METRI_ORIGIN_TOKEN no configurado (nil) → no bloquea ningún request"
  (are [headers] (nil? (validate-zero-trust headers nil))
    {}
    {"x-metri-origin-token" "anything"}
    {"x-metri-origin-token" ""}))

(deftest zt-06-extract-token-string-key
  "ZT-06 — extract-token maneja header como string lowercase (Lambda normaliza)"
  (let [headers {"x-metri-origin-token" stack-id-example}]
    (is (= stack-id-example (extract-token headers)))))

(deftest zt-07-extract-token-keyword-key
  "ZT-07 — extract-token maneja header como keyword (Cheshire parsea JSON con true)"
  (let [headers {:x-metri-origin-token stack-id-example}]
    (is (= stack-id-example (extract-token headers)))))

(deftest zt-08-token-es-arn-cloudformation
  "ZT-08 — El token de producción tiene forma de ARN CloudFormation
   Formato: arn:aws:cloudformation:{region}:{account}:stack/{name}/{uuid}"
  (let [cfn-arn-pattern #"arn:aws:cloudformation:[a-z0-9-]+:\d+:stack/[a-zA-Z0-9-]+/.+"]
    (is (re-matches cfn-arn-pattern stack-id-example)
        "El StackId de ejemplo debe tener formato ARN CloudFormation válido")))

;; ─── Invariantes de seguridad ─────────────────────────────────────────────────

(deftest zt-invariant-403-shape
  "ZT-INVARIANT — Toda respuesta 403 tiene :status 403 y :body con :message"
  (doseq [bad-headers [{} {"x-metri-origin-token" "wrong"} {"x-metri-origin-token" ""}]]
    (let [result (validate-zero-trust bad-headers stack-id-example)]
      (is (= 403 (:status result)) (str "Debe ser 403 para headers: " bad-headers))
      (is (string? (get-in result [:body :message])) "Debe tener :body/:message string"))))
