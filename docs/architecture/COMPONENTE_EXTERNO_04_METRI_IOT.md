# Componente Externo 04 - Módulo Metri IoT y Eventos Telemetricos

La telemetría en Metri Engine no satura las líneas vitales operativas. Todo evento físico de maquinaria entra a través del **Componente Metri IoT**, separado orgánicamente como Serverless.

## 1. El Catálogo Maestro IoT (Datahike)
El Catálogo separa estéticamente el Activo Físico tradicional (`asset.json`) de su conexión a la nube MQTT, previniendo dañar el inventario en rotación de hardware de proveedores.
1. **Vinculación Abstraída:** La interfaz web inscribe una suscripción en Datahike, conectando el Activo a un `topic_pattern` estandarizado (Ej. `metri/telemetry/{tenant_id}/{asset_id}/#`). Al tratar al IoT como una entidad de `engine: oltp`, hereda la auditoría inmutable Time-Travel de cualquier otra entidad financiera.

## 2. Stateful Multiplexer: Evasión Crítica de Límites AWS
El mayor talón de Aquiles de plataformas SaaS apoyadas en la suite nativa generalista es el temido "Quota Limit". En particular, la asfixiante cuota y costo de _AWS IoT Rules_ (1,000 cuotas por cuenta técnica).

Para un inquilino multi-país que debe apagar turbinas si tiemblan bajo latencias extremas (`VIBRATION > 10.0`), las latencias de encolado convencional matan el negocio.
- **Solución Matemática:** Impusimos el paradigma invencible **Stateful Multiplexer** totalmente exento de cuotas. Todos los eventos MQTT masivos desembocan crudos o decantados a un Lambda Harvester unificado en RAM. Este Mux invoca lógica embebida sobre su propia rama y canaliza la alarma por EventBridge asíncronamente; eludiendo para siempre las Reglas Pagas restrictivas de AWS IoT Core, habilitando millones de Activos concurrentes sin rebasar límites pre-pactados. 
- Erradicamos dependientemente la asfixiante estructura paralizante de **AWS IoT Device Shadow**. Abstraímos el "Gemelo Digital" inyectando estatus en tiempo real en nuestro motor in-memory local y decantando en el Storage Base Datahike.

## 3. Envelope Encryption de Llaves
- **Decodificación KMS Autónoma:** Cuando Lambda Harvester debe emitir comandos inversos para cerrar válvulas Físicas a integradores X, extrae las credenciales del API de la tabla `iot_harvester_config.json`, decodificándolas sub-milisegundo vía KMS Key. Esto impide robos o dependencias paralizantes HTTP al caro e inaccesible _AWS Secrets Manager_, proveyendo seguridad criptográfica con microsegundos de lectura asíncrona.
