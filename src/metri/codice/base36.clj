;; [PORTED_TO_RUST: src/codice/base36.rs]
;; NO MODIFICAR — fuente de verdad en Rust
(ns metri.codice.base36
  "FASE 02 — MÓDULO IV: Generador estocástico Base36.
   Genera códigos alfanuméricos únicos con SecureRandom.
   Función pura — cero I/O, cero Datahike, cero side-effects.
   Usada por generator/inject! para atributos con strategy:stochastic_base36.")

;; ── Alfabeto canónico Base36 ──────────────────────────────────────────────
;; 10 dígitos + 26 letras mayúsculas = 36 símbolos.
;; P(colisión con length=7) < 1/36^7 ≈ 1 en 78.000 millones.
(def ^:private ALPHABET "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
(def ^:private ALPHA-LEN (count ALPHABET))

;; Singleton SecureRandom — thread-safe, cero re-inicialización por request.
(def ^:private ^java.security.SecureRandom rng (java.security.SecureRandom.))

;; ── generate — punto de entrada público ──────────────────────────────────
;;
;; Entrada:  prefix string (ej: "A-", "L-", "WO-")
;;           length int    (ej: 7)
;; Salida:   string = prefix + length caracteres Base36
;;           ej: "A-X7K2M9P"
;;
;; Garantías:
;;   ✔ Función pura (excepto SecureRandom — efectuado localmente, sin I/O externo)
;;   ✔ Thread-safe — SecureRandom es sincronizado
;;   ✔ Unicidad probabilística: P(colisión length=7) < 1/36^7
;;   ✔ No predecible — SecureRandom, no Math/random
(defn generate
  "Genera un código alfanumérico Base36 de longitud `length` con prefijo `prefix`.
   Ejemplo: (generate \"A-\" 7) → \"A-X7K2M9P\""
  [^String prefix ^long length]
  (let [chars (repeatedly length
                           #(str (nth ALPHABET (.nextInt rng ALPHA-LEN))))]
    (str prefix (apply str chars))))
