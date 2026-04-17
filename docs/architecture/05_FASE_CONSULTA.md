# Fase 05 - Borde de Consulta (Read Path) y Optimizaciones Aegis/Janus

El Orquestador **Janus** y su Motor de Transpilación Polimórfica **Aegis** establecen una barrera de alto rendimiento entre la Petición gRPC del cliente y los motores transaccionales físicos (Datahike/Athena), protegiendo la latencia y aplicando mitigaciones matemáticas.

## 1. El Bypass Zero-Ciclo (API Nativa)
- **Transmutación en Aegis (Zero-Ciclo):** De igual forma, si Aegis intercepta una petición UI y percibe que NO hay "llaves de matemáticas" (ausencia total de `measures` o `metrics`), cancela la ruta OLAP y asume que es una clásica *Lectura Operacional de Entidad*. Delega la responsabilidad anulando el AST intermedio y utilizando directamente la API Pull de Datahike para máxima velocidad:
  E.j: `d/pull [{:labor_log_ids [*]} {:completed_by [*]}]`.

## 2. Distincts Predictivos al Vuelo (Esquemas Enriquecidos)
- **Hipertipo Classification (UI Filters):** Si un catálogo de UI pide autocompletado y el atributo ostenta el hiper-tipo en JSON de `"subtype": "classification"` (Ej. `tag1`, `status_code`), **Aegis** compila transaccionalmente en memoria un estatuto especial `DISTINCT`. Al barrer los índices reales transaccionales AVET de Datahike, retorna el 100% de los valores únicos inyectados históricamente de un golpe, dotando al Frontend UX de filtros predictivos al vuelo sin necesidad de maestros rígidos administrados por usuarios.

## 3. Transpilación a Datalog y Optimización Subyacente
- **Cruce Cartesiano: Dijkstra Pathfinding:** En el modo **OLTP (Aegis-Datahike)**, Aegis compila y machaca los vectores de filtrado (`FilterNode` del cliente) inyectando obligatoriamente las lógicas analíticas de abstracción con sintaxis EDN. Inserta heurísticas de _Dijkstra Pathfinding_ a los grafos para evadir estricta y dolorosamente cruces cartesianos inyectados por peticiones maliciosas (ej. un JOIN múltiple infinito pidiendo N:N:N variables transaccionales).

## 4. O(1) T-Digest (Estadística de Mediana y Cuantiles)
- **Bloqueo contra Escaneos de RAM Masivos (`d/q`):** Si Janus enruta a OLTP, prohíbe severamente escaneos de subconsulta infinitos con la macro convencional `d/q`. En su lugar, intercepta los instantes Unix Epoch y ejecuta flujos de iteración AVET puros de bajo nivel de Datahike (`d/datoms`), saltándose la virtualización Java/JVM por materializaciones O(1).
- **T-Digest Algorithm:** Al pedir la UI Cuantiles o percentiles ríspidos (ej. 'Mediana perfecta de costos trimestrales'), el motor evita un Order-By y transacciona el algoritmo estadístico **T-Digest en O(1)** construyendo los buckets matemáticos logarítmicamente para resultados cuasi-instantáneos en latencia de servidor.

## 5. Evasión de Mutilación Web (Bypass WebGL / IPC)
- **Transferencia Cruda (Arrow Blob Bypass):** Si el motor Athena Cloud o los repositorios AWS S3 terminan consolidando reportes masivos de gigabytes y Iceberg Tabular, Janus _se niega a parsearlos a JSON Stream_. El proceso intermedio es saltado completamente (Bypass WebGL). Extrae la cola de registros o los formatos _Apache Arrow Binary Blobs_ crudos, les inyecta un Header en formato `VizMeta` y los deriva nativamente por HTTP2 Data gRPC a la computadora final, mitigando por completo un cuello de botella fatal CPU Node.js / API Gateway.
