(ns metri.aegis.pagination
  "Paginación por offset codificado en Base64.

   Estrategia: cursor = Base64(\"<offset>:<limit>\")
     cursor = nil / \"\" → página 1, offset = 0
     cursor = \"MTA6MTA=\" → Base64(\"10:10\") → offset=10, limit=10 (página 2)
     cursor = \"MjA6MTA=\" → Base64(\"20:10\") → offset=20, limit=10 (página 3)

   SRP: módulo exclusivo de paginación — sin dependencias de negocio.
   Puro: todas las funciones son puras (sin side-effects).

   Uso:
     (decode-cursor nil)          ;; → {:offset 0 :limit 50}
     (decode-cursor \"MTA6MTA=\")  ;; → {:offset 10 :limit 10}
     (build-pagination {:offset 10 :limit 10 :total 90})
     ;; → {:page-size 10 :has-next true :has-previous true
     ;;    :next-cursor \"MjA6MTA=\" :previous-cursor \"\"}")

;; ── Encoder / Decoder ─────────────────────────────────────────────────────────

(defn decode-cursor
  "Decodifica un cursor Base64(offset:limit) en un mapa {:offset :limit}.
   Retorna {:offset 0 :limit fallback-limit} cuando el cursor es nil/vacío
   o cuando el formato es inválido (defensa contra cursor manipulado).

   No lanza excepción — siempre retorna un mapa válido."
  ([cursor] (decode-cursor cursor 50))
  ([cursor fallback-limit]
   (if (seq cursor)
     (try
       (let [decoded (String. (.decode (java.util.Base64/getDecoder) ^String cursor))
             parts   (clojure.string/split decoded #":" 2)]
         (when (= 2 (count parts))
           {:offset (Long/parseLong (first parts))
            :limit  (Long/parseLong (second parts))}))
       (catch Exception _
         {:offset 0 :limit fallback-limit}))
     {:offset 0 :limit fallback-limit})))

(defn encode-cursor
  "Codifica offset + limit en un cursor Base64(offset:limit).
   Retorna nil cuando offset < 0 (no hay página anterior)."
  [offset limit]
  (when (and (some? offset) (>= offset 0))
    (.encodeToString (java.util.Base64/getEncoder)
                     (.getBytes (str offset ":" limit) "UTF-8"))))

;; ── Builder de paginación ─────────────────────────────────────────────────────

(defn build-pagination
  "Construye el mapa :pagination con cursores reales basados en offset.

   Parámetros:
     :offset  — posición actual en el dataset (número de filas a saltar)
     :limit   — tamaño de página solicitado
     :total   — total de registros disponibles ANTES de paginar

   Retorna un mapa compatible con metres.janus.normalizer → build-pagination:
     :page-size       int
     :has-next        boolean
     :has-previous    boolean
     :next-cursor     string | ausente cuando no hay siguiente
     :previous-cursor string | ausente cuando no hay anterior"
  [{:keys [offset limit total]}]
  (let [offset       (or offset 0)
        limit        (or limit 50)
        total        (or total 0)
        has-next     (< (+ offset limit) total)
        has-previous (> offset 0)
        next-offset  (+ offset limit)
        prev-offset  (max 0 (- offset limit))]
    (cond-> {:page-size    limit
             :has-next     has-next
             :has-previous has-previous}
      has-next     (assoc :next-cursor     (encode-cursor next-offset limit))
      has-previous (assoc :previous-cursor (encode-cursor prev-offset limit)))))

;; ── Slicer ────────────────────────────────────────────────────────────────────

(defn paginate-rows
  "Aplica offset + limit a un vector de rows ya ordenados.
   Equivalente a SQL: OFFSET offset LIMIT limit.

   Retorna un vector (nunca nil)."
  [rows offset limit]
  (vec (take limit (drop (or offset 0) rows))))
