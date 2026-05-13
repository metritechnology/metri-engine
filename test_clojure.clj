(require '[metri.grpc.translator :as t])
(import '[metri.data.grpc QueryRequest AnalyticsRequest])

(let [q-builder (AnalyticsRequest/newBuilder)
      _ (.setTenantId q-builder "golden-tenant-1234")
      _ (.setEntity q-builder "inventory_movement")
      q (.build q-builder)
      req-builder (QueryRequest/newBuilder)
      _ (.setTenantId req-builder "golden-tenant-1234")
      _ (.putQueries req-builder "oltp_table" q)
      req (.build req-builder)
      ctx (t/query-request->ctx req)]
  (println "CTX QUERIES:")
  (println (:queries ctx)))
