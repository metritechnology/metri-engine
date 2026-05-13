(ns metri.janus-router.ulid
  "Wrapper sobre com.github.f4b6a3/ulid-creator 5.2.4.
   Librería de referencia JVM para ULID — 0 dependencias transitivas,
   monotónica, thread-safe, >10M descargas Maven Central.
   Spec: https://github.com/ulid/spec

   Formato: ttttttttttrrrrrrrrrrrrrrrrr  (26 chars Crockford Base32)
            ├─ 48 bits timestamp (ms epoch) — lexicográficamente ordenable
            └─ 80 bits random — unicidad global

   API pública:
     (ulid/generate)   → String  '01HQZK7GXFM5AZBVDNWEJTR4Q3'
     (ulid/ulid? s)    → boolean"
  (:import [com.github.f4b6a3.ulid UlidCreator]))

;; ── Generador monotónico global (thread-safe internamente) ────────────────
;; UlidCreator.getMonotonicUlid() garantiza orden estricto incluso dentro
;; del mismo milisegundo: incrementa los bits random en lugar de regenerarlos.
(defn generate
  "Genera un ULID monotónico de 26 caracteres Crockford Base32.
   Thread-safe. Nunca lanza. Ej: '01HQZK7GXFM5AZBVDNWEJTR4Q3'"
  ^String []
  (-> (UlidCreator/getMonotonicUlid)
      (.toString)))

;; ── Validación ────────────────────────────────────────────────────────────
(def ^:private ^String CROCKFORD "0123456789ABCDEFGHJKMNPQRSTVWXYZ")

(defn ulid?
  "Retorna true si s es un ULID válido (26 chars Crockford Base32)."
  [s]
  (and (string? s)
       (= 26 (count s))
       (every? #(>= (.indexOf CROCKFORD (str (Character/toUpperCase ^char %))) 0) s)))
