# Fase 08 - Inteligencia Artificial Model Context Protocol (MCP)

**Metri MCP** abandona la invocación clásica insegura de los modelos GPT mediante APIs en línea directa en el core. Implementamos un microservicio Serverless (TypeScript) puente basado en el estándar del mercado **Model Context Protocol (MCP)**, blindando a Metri Engine contra alucinaciones del modelo y la pesada ejecución semántica.

## 1. Desacople SSE (Server Sent Events)
- Alojar llamadas streaming en el hilo nativo de Clojure JVM provocaría _Timeouts_. El proxy MCP TypeScript ataja todos los streams LLM, actuando de parachoques de la red. Se comunica con Clojure a través de peticiones gRPC rápidas (Machine-to-Machine `M2M`). Esto asegura inmunidad de latencias de Vendor IA (OpenAI, Anthropic).

## 2. Exploración Dinámica (Discovery `mcp-inspector`)
- La IA averigua orgánicamente y "en frío" la taxonomía y bases del tenant consultando mediante `rpc Discovery`. Esto implementa la filosofía Data-First (cero-prompts manuales hardcodeados en infra que generan filtración técnica de datos).

## 3. Framework Zero-Trust y OTel Censorship
1. Las IA a menudo alucinan parámetros. Si el modelo trata de invocar funciones Zod incorrectas, el Proxy intercepta con `ZodValidation` y emite un `Result Error Code` formateado, jamás quebrando el proceso.
2. Simultáneamente, el evento se loguea como un `Warning` al Security Trace Bus vía OTel.
3. El proxy **Censura** y expurga pasivamente todo el Prompt y Output extrayendo entidades PII o números de tarjetas de identificación (Data-Leak Sanitization) previo al regreso al usuario garantizando Compliance corporativo absoluto.
