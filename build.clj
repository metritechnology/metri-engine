(ns build
  (:require [clojure.tools.build.api :as b]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(def lib       'metri/engine)
(def version   (format "1.0.%s" (b/git-count-revs nil)))
(def class-dir "target/classes")
(def proto-dir "target/proto-src")
(def basis     (b/create-basis {:project "deps.edn"}))
(def uber-file "target/metri-engine.jar")

(defn- sh! [& args]
  (let [proc (-> (ProcessBuilder. ^java.util.List (vec args))
                 (.inheritIO)
                 (.start))]
    (when-not (zero? (.waitFor proc))
      (throw (ex-info (str "Command failed: " (str/join " " args)) {})))))

(defn clean [_]
  (b/delete {:path "target"}))

(defn compile-proto [_]
  (println "▶ Compilando metres.proto...")
  (io/make-parents (io/file proto-dir "placeholder"))
  (let [home        (System/getenv "HOME")
        ;; protoc 29.3 genera código compatible con protobuf-java 4.29.3 + Java 25
        ;; Matriz: protoc 29.x ↔ protobuf-java 4.29.x ↔ grpc-java 1.73.0
        protoc-bin  (or (System/getenv "PROTOC_BIN") (str home "/.local/protoc-29/bin/protoc"))
        include-wkt (or (System/getenv "PROTOC_INC") (str home "/.local/protoc-29/include"))
        plugin-path (or (System/getenv "PROTOC_PLUGIN") (str home "/.local/bin/protoc-gen-grpc-java-1.73"))]
    (sh! protoc-bin
         (str "--plugin=protoc-gen-grpc-java=" plugin-path)
         "--java_out"      proto-dir
         "--grpc-java_out" proto-dir
         "--proto_path"    "."
         "--proto_path"    include-wkt
         "metri.proto"))
  (println "  ✓ Protobuf compilado en" proto-dir))

(defn compile-java! [_]
  (println "▶ Compilando Java generado (protoc)...")
  (let [java-files (->> (file-seq (io/file proto-dir))
                        (filter #(.endsWith (.getName %) ".java"))
                        (mapv #(.getAbsolutePath %)))
        ;; (:classpath-roots basis) = todos los JARs del grafo de dependencias
        classpath  (str/join java.io.File/pathSeparator
                             (cons class-dir (:classpath-roots basis)))]
    (when (seq java-files)
      (apply sh! "javac"
             "--release" "11"   ;; sets system modules path automatically
             "-d"        class-dir
             "-cp"       classpath
             java-files)))
  (println "  ✓ Java compilado"))

(defn uber [_opts]
  (clean nil)
  ;; SIEMPRE recompilar el proto desde fuente para evitar desfase proto/JAR.
  ;; Fallback al JAR anterior solo si protoc no está disponible en el PATH.
  (let [home       (System/getenv "HOME")
        protoc-bin (or (System/getenv "PROTOC_BIN") (str home "/.local/protoc-29/bin/protoc"))
        protoc-ok? (let [f (io/file protoc-bin)]
                     (if (.isAbsolute f)
                       (.exists f)
                       (zero? (.waitFor (.start (ProcessBuilder. ["which" protoc-bin]))))))]
    (if protoc-ok?
      (do
        (println "▶ Recompilando metres.proto desde fuente (protoc disponible)...")
        (compile-proto nil)
        (compile-java! nil))
      ;; Protoc no disponible: restaurar desde JAR anterior si existe
      (let [last-jar (io/file ".aws-sam/build/MetriEngineFunction/lib/metri-engine.jar")]
        (if (.exists last-jar)
          (do
            (println "⚠️  protoc no encontrado. Restaurando clases Protobuf desde" (.getPath last-jar))
            (sh! "bash" "-c" (str "mkdir -p target/classes && cd target/classes && jar xf " (.getAbsolutePath last-jar) " metri/data/grpc")))
          (throw (ex-info "No se puede construir: protoc no disponible y no existe JAR anterior." {}))))))

  ;; 2. Copiar src y resources al class-dir
  (b/copy-dir {:src-dirs   ["src" "resources"]
               :target-dir class-dir})
  ;; 3.5 Extraer protobuf-java 4.29.3 en class-dir para que sobreescriba
  ;;     versiones embebidas en grpc-netty (que trae protobuf 3.x/4.x antiguo)
  (println "▶ Imponiendo protobuf-java 4.29.3 en uberjar...")
  (let [pb-jar (some #(when (.contains (str %) "protobuf-java-4.29.3.jar") %)
                     (:classpath-roots basis))]
    (when pb-jar
      (let [proc (-> (ProcessBuilder. ["jar" "xf" (str pb-jar)])
                     (.directory (io/file class-dir))
                     (.start))]
        (.waitFor proc))
      (println "  ✓ protobuf-java 4.29.3 extraído en" class-dir)))
  ;; 4. Compilar Clojure (AOT estricto solo en entrypoints para evitar bugs de core.async)
  (b/compile-clj {:basis     basis
                  :src-dirs  ["src"]
                  :class-dir class-dir
                  :ns-compile '[metri.main metri.lambda.handler]})
  ;; 5. Empaquetar uberjar
  ;; NOTA: grpc-netty-shaded incluye protobuf 3.x que sobrescribiría protobuf 4.x.
  ;; Se excluyen esas clases para que el uberjar use protobuf-java 4.29.3.
  (b/uber {:class-dir class-dir
           :uber-file uber-file
           :basis     basis
           :main      'metri.main
           :exclude   [#"META-INF/license/.*"
                       #"META-INF/LICENSE.*"
                       #"META-INF/NOTICE.*"
                       #"META-INF/DEPENDENCIES.*"
                       #"module-info\.class"]})
  (println (str "✓ Uberjar: " uber-file)))

(defn native [_]
  (uber nil)
  (println "Execute GraalVM native-image on" uber-file))
