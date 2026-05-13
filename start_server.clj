(require '[metri.grpc.server :as server])
(require '[integrant.core :as ig])

(def system-config
  {:grpc/health-manager {}
   :grpc/server {:port 9090
                 :service-impl (proxy [metri.data.grpc.MetriServiceGrpc$MetriServiceImplBase] []
                                 (query [req stream]
                                   (println "Received query req:" req)
                                   (.onNext stream (metri.data.grpc.QueryResponse/getDefaultInstance))
                                   (.onCompleted stream)))
                 :health-manager (ig/ref :grpc/health-manager)}})

(println "Starting mock server...")
(def sys (ig/init system-config))
(println "Server started on port 9090")
(Thread/sleep 30000)
