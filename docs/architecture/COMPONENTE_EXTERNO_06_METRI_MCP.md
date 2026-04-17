# Componente Externo 06 - Metri MCP Proxy (AI Connector)

El **Metri MCP Proxy** es una aplicación independiente Serverless elaborada puramente en TypeScript / Node.js. Su mandato único es intermediar el denso y propenso-a-desconexión diálogo bidireccional HTTP y Streams (SSE) típico de los Modelos de Lenguaje Granes (LLMs), frente al robusto y estricto backend transaccional de metadatos Clojure (`Metri Engine`).

## 1. El Parachoques de Alta Fricción Transaccional (gRPC Shielding)
Los LLMs tardan entre 4 a 15 segundos en inferir secuencias y sufren frecuentes latencias cognitivas.
- Si incrustáramos este procesamiento en las bases del motor RAM de Clojure u ofreciéramos un socket abierto sincrónico, los hilos primarios se agotarían por el _Connection Timeout_ impidiendo la subida de los recolectores de costos empresariales.
- El TypeScript Proxy funciona recibiendo un Stream o WebSocket desde la Interfaz de la IA (Front-End) y mantiene esa conexión caliente y barata. Por detrás y en nanosegundos el Proxy extrae los metadatos de intención y envía descargas rápidas compiladas llamando al contrato protobuf de Clojure `rpc Discovery`, `rpc Explore` y `rpc Query` por **gRPC HTTP2**. 
- La arquitectura JVM retorna la matriz transaccional en un segundo y libera inmediatamente los recursos para transacciones de negocio normales, dejando al Proxy TypeScript encargado de parsear y responderle gradualmente al humano por SSE.

## 2. Abstracción del Proveedor AI y el Protocolo Anthropic MCP
Mantenemos estricto desacoplamiento sobre la elección de la I.A. Todo el flujo proxy de este componente obedece la firma estándar del _Model Context Protocol (MCP)_.
Esto significa que si en el año futuro decidimos reemplazar GPT-4 por un agente de código abierto Mistral-LLaMA, no tocamos ni una coma del backend Clojure, y garantizamos inmunidad de API Vendors.
