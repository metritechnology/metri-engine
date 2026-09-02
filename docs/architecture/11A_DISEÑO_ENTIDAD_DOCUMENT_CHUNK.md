# Anexo 11A: Diseño de la Entidad Document Chunk (Búsqueda Semántica RAG)

Este anexo detalla el diseño de la entidad `document_chunk`, la cual actúa como el puente de almacenamiento para documentos particionados (como fichas técnicas de plantas eléctricas) en la arquitectura de búsqueda vectorial de Metri.

---

## 1. Definición del Modelo de Datos

La configuración formal del modelo se encuentra en [document_chunk.json](file:///Users/macuser/projects/metri/metri-engine/config/models/document_chunk.json).

### Esquema Conceptual

*   **Identificador Único (`id`):** Un UUID aleatorio que identifica de forma unívoca a este fragmento específico del documento.
*   **Referencia al Documento (`parent_file`):** Relación de cardinalidad `one` apuntando a la entidad `file`. Permite recuperar la URL del PDF, metadatos y nombre original del archivo fuente para fines de citación en las respuestas de la IA.
*   **Índice del Fragmento (`chunk_index`):** Un entero secuencial (0, 1, 2...) que permite ordenar de forma lógica la secuencia de la ficha técnica si se requiere reconstruir o leer fragmentos adyacentes.
*   **Contenido de Texto (`chunk_content`):** El bloque textual extraído (generalmente limitado a rangos de ~500 a 1000 tokens para balancear la precisión semántica y el costo del contexto del LLM).
*   **Embedding Vectorial (`semantic_embedding`):** Un vector denso de 1024 dimensiones, generado de forma automática a partir de `chunk_content` mediante la integración con `amazon.titan-embed-text-v2`.
*   **Etiquetas de Metadatos (`metadata_tags`):** Categorizaciones secundarias útiles para el filtrado previo a la búsqueda vectorial (ej. pre-filtrar solo fragmentos marcados como `'electrical'` o `'maintenance'`).

---

## 2. Ciclo de Vida del Fragmento (RAG Pipeline)

### 2.1 Ingesta y Fragmentación (Write Path)
Cuando se carga un documento (ej. `ficha_tecnica_planta_electrica.pdf`):
1.  **Procesamiento:** Un servicio extractor parsea el PDF y divide el texto en fragmentos (chunks) lógicos (respetando finales de párrafo o tablas).
2.  **Transacción Metri:** Por cada fragmento, se invoca una mutación en `metri-engine` con la entidad `document_chunk`.
3.  **Generación Asíncrona:** El engine invoca a Bedrock a través del MCP Proxy. El embedding resultante se almacena en el campo `semantic_embedding` en DynamoDB y S3.

### 2.2 Consulta y Recuperación (Read Path)
Cuando un usuario pregunta: *¿Qué tipo de aceite usa la planta eléctrica?*
1.  **Vectorización del Query:** Se genera el vector de la pregunta con Titan Embeddings v2.
2.  **Filtro y Búsqueda KNN:**
    *   Se filtra estrictamente por el `tenant_id` del usuario.
    *   Se realiza la búsqueda sobre los índices HNSW efímeros almacenados en S3 (Camino A) o mediante Athena en consultas analíticas (Camino C).
3.  **Filtrado por Políticas Cedar (ABAC):** Antes de enviar los fragmentos resultantes al LLM, el PDP de Cedar evalúa si el rol del usuario le permite ver el archivo asociado (`parent_file`).
4.  **Inferencia RAG:** Los fragmentos aprobados se envían como contexto estructurado a **AWS Bedrock Nova** para la generación de la respuesta final.
