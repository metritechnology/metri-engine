(ns metri.infrastructure.session-store
  "Session Store — HMAC Token Auto-contenido.

   Responsabilidad única: verificar que un token 'mk_...' fue firmado por Metri
   y extraer la identidad {tenant-id, user-id} que Cedar necesita para autorizar.

   Lo que NO hace este namespace:
   - No evalúa permisos ni roles → responsabilidad de Cedar (iop/cedar-authorizer)
   - No persiste sesiones → el token ES la sesión (auto-contenido)
   - No gestiona API keys → responsabilidad del registro de tenants (Datahike)

   Token format:
     mk_<base64url({tid, uid, iat, exp, jti})>.<base64url(HMAC-SHA256)>

   Verificación: HMAC-SHA256 local — <0.1ms, $0, sin red.
   Secret: AWS Secrets Manager (HMAC_SECRET_ARN), cargado una vez en SnapStart.
   Revocación masiva: rotar el secret → todos los tokens inválidos al instante.
   Revocación individual: REVOKED#<jti> en DynamoDB con TTL nativo.

   Ciclo de vida: Integrant (:infra/session-store)."
  (:require [integrant.core :as ig]
            [taoensso.timbre :as log]
            [cheshire.core :as json]
            [cognitect.aws.client.api :as aws]
            [metri.domain.protocols :as proto]
            [metri.domain.errors :as errors]
            [metri.infrastructure.dynamodb :as ddb])
  (:import [javax.crypto Mac]
           [javax.crypto.spec SecretKeySpec]
           [java.security MessageDigest]
           [java.util Base64]))

;; ══════════════════════════════════════════════════════════════════════════════
;; Primitivas criptográficas — JVM puro, sin dependencias externas
;; ══════════════════════════════════════════════════════════════════════════════

(defn- b64url-encode ^String [^bytes bs]
  (.encodeToString (Base64/getUrlEncoder) bs))

(defn- b64url-decode ^bytes [^String s]
  (.decode (Base64/getUrlDecoder) s))

(defn- hmac-sha256 ^bytes [^bytes secret ^bytes message]
  (let [key (SecretKeySpec. secret "HmacSHA256")
        mac (doto (Mac/getInstance "HmacSHA256") (.init key))]
    (.doFinal mac message)))

(defn- constant-time-eq? [^bytes a ^bytes b]
  "Comparación en tiempo constante — previene timing attacks."
  (MessageDigest/isEqual a b))

;; ══════════════════════════════════════════════════════════════════════════════
;; API pública — issue y verify
;; ══════════════════════════════════════════════════════════════════════════════

(defn issue-token
  "Emite un token HMAC firmado con identidad mínima.
   Retorna: 'mk_<base64url(payload)>.<base64url(HMAC)>'
   Solo incluye identidad — Cedar resuelve permisos desde Datahike."
  [^bytes secret {:keys [tenant-id user-id ttl-seconds jti]
                  :or   {ttl-seconds 3600
                         jti         (str (java.util.UUID/randomUUID))}}]
  (let [now           (quot (System/currentTimeMillis) 1000)
        payload-bytes (.getBytes (json/generate-string
                                   {:tid tenant-id
                                    :uid user-id
                                    :iat now
                                    :exp (+ now ttl-seconds)
                                    :jti jti})
                                 "UTF-8")
        payload-b64   (b64url-encode payload-bytes)
        sig-b64       (b64url-encode (hmac-sha256 secret payload-bytes))]
    (str "mk_" payload-b64 "." sig-b64)))

(defn- verify-signature
  "Verifica firma y expiración. Retorna {:tenant-id :user-id :jti :exp} o nil.
   No consulta blacklist — eso lo hace get-session."
  [^bytes secret ^String raw-token]
  (try
    (when (clojure.string/starts-with? raw-token "mk_")
      (let [[payload-b64 sig-b64] (clojure.string/split (subs raw-token 3) #"\." 2)
            payload-bytes         (b64url-decode payload-b64)
            expected-sig          (hmac-sha256 secret payload-bytes)
            provided-sig          (b64url-decode sig-b64)]
        (when (constant-time-eq? expected-sig provided-sig)
          (let [claims (json/parse-string (String. payload-bytes "UTF-8") true)
                now    (quot (System/currentTimeMillis) 1000)]
            (when (> (:exp claims) now)
              {:tenant-id (:tid claims)
               :user-id   (:uid claims)
               :jti       (:jti claims)
               :exp       (:exp claims)})))))
    (catch Exception e
      (log/debug "[HMAC] Firma inválida:" (ex-message e))
      nil)))

;; ══════════════════════════════════════════════════════════════════════════════
;; Blacklist — solo para revocaciones individuales (raro)
;; PK: "REVOKED#<jti>"  ttl: expiry original del token
;; ══════════════════════════════════════════════════════════════════════════════

(defn- blacklisted? [ddb-client table-name jti]
  (when (and ddb-client table-name jti)
    (let [result (ddb/get-item ddb-client table-name {:PK {:S (str "REVOKED#" jti)}})]
      (and (= :ok (first result)) (some? (second result))))))

;; ══════════════════════════════════════════════════════════════════════════════
;; HMACTokenStore — implementa ISessionStore
;; ══════════════════════════════════════════════════════════════════════════════

(defrecord HMACTokenStore [secret ddb-client table-name]
  proto/ISessionStore

  (get-session [_ raw-token]
    ;; Happy path: verificación local <0.1ms, $0.
    ;; Solo va a DynamoDB si el token tiene firma válida y no expiró (blacklist check).
    (when-let [claims (verify-signature @secret raw-token)]
      (if (blacklisted? ddb-client table-name (:jti claims))
        (do (log/warn "[HMAC] Token revocado, jti:" (:jti claims)) nil)
        claims)))

  (put-session! [_ raw-token _ ttl-seconds]
    ;; put-session! = REVOCAR: añade el jti a la blacklist DynamoDB.
    ;; Se llama en logout forzoso, ban de usuario, o rotación de API key individual.
    (try
      (when (and ddb-client table-name)
        (let [claims  (verify-signature @secret raw-token)
              jti     (or (:jti claims)
                          (b64url-encode (.getBytes ^String raw-token "UTF-8")))
              expires (+ (quot (System/currentTimeMillis) 1000) (or ttl-seconds 86400))]
          (log/info "[HMAC] Revocando token, jti:" jti)
          (ddb/put-item! ddb-client table-name
                         {:PK  {:S (str "REVOKED#" jti)}
                          :ttl {:N (str expires)}})))
      :ok
      (catch Exception e
        (log/error "[HMAC] Revocación falló:" (ex-message e))
        (errors/error :INFRA_SESSION_001 {:detail (ex-message e)}))))

  (del-session! [_ jti]
    ;; Quita un jti de la blacklist (des-revocar — caso extremadamente raro).
    (when (and ddb-client table-name)
      (ddb/delete-item! ddb-client table-name {:PK {:S (str "REVOKED#" jti)}}))
    :ok))

;; ══════════════════════════════════════════════════════════════════════════════
;; Integrant Lifecycle
;; ══════════════════════════════════════════════════════════════════════════════

(defn- load-secret [secret-arn region dev-secret]
  (delay
    (cond
      secret-arn
      (do
        (log/info "  -> [HMAC] Cargando secret desde Secrets Manager:" secret-arn)
        (let [sm   (aws/client {:api :secretsmanager :region region})
              resp (aws/invoke sm {:op :GetSecretValue :request {:SecretId secret-arn}})]
          (when (:cognitect.anomalies/category resp)
            (throw (ex-info "No se pudo cargar HMAC secret" {:arn secret-arn :anomaly resp})))
          (.getBytes ^String (:SecretString resp) "UTF-8")))

      dev-secret
      (do
        (log/warn "  -> [HMAC] Secret de DESARROLLO fijo — NUNCA en producción")
        (.getBytes ^String dev-secret "UTF-8"))

      :else
      (throw (ex-info "HMAC requiere :secret-arn (prod) o :dev-secret (dev)" {})))))

(defmethod ig/init-key :infra/session-store
  [_ {:keys [secret-arn region dev-secret ddb-client table-name]}]
  (let [secret (load-secret secret-arn region dev-secret)]
    (log/info "  -> [HMAC] Session store listo — verificación local, sin red")
    (->HMACTokenStore secret ddb-client table-name)))

(defmethod ig/halt-key! :infra/session-store [_ _]
  (log/info "  -> [HMAC] Session store cerrado"))
