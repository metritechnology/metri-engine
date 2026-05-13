# Integración de Label Templates (Frontend)

Este documento define el contrato de visualización y el manejo de plantillas de etiquetas (`label_template`) emitidas por el **Metri Engine** hacia la capa de presentación (Frontend/ECharts).

El backend optimiza el rendimiento dividiendo la responsabilidad de la interpolación dependiendo de la estrategia gráfica solicitada.

---

## 1. Gráficos Circulares (Pie / Donut)
Para visualizaciones que agrupan métricas en porciones finitas (ej. Pie), el Metri Engine realiza una **Interpolación Server-Side**. 

Esto garantiza que las llaves principales del diccionario ya vengan resueltas con el template aplicado, facilitando el renderizado en componentes que asumen la llave como el nombre final.

### Comportamiento
- El campo `label_template` **NO** se envía en la configuración `viz_ext`.
- El backend evalúa la plantilla solicitada internamente.
- El texto final aparece directamente como la llave (key) dentro de `breakdown.signals`.

### Ejemplo de Payload (Backend → Frontend)
**Template Solicitado:** `"Tipo: {{type}}"`
```json
"viz_ext": {
  "type": "pie",
  "breakdown": {
    "signals": {
      "Tipo: RECEIPT": { "value": 129.0 },
      "Tipo: ISSUE": { "value": 98.0 }
    }
  }
}
```
**Instrucción para Frontend:** No se requiere procesamiento adicional. Iterar sobre las llaves de `signals` y utilizarlas directamente como las etiquetas (labels) del gráfico.

---

## 2. Gráficos de Coordenadas (Line / Bar / Scatter)
Para series temporales y diagramas de dispersión, que pueden contener decenas de miles de puntos de datos, el Metri Engine aplica una **Delegación Client-Side**. 

Si el backend inyectara el texto estático en cada fila, el tamaño del *payload* se multiplicaría drásticamente. Por ende, se envía un **molde** global y el frontend es responsable de ensamblarlo dinámicamente.

### Comportamiento
- El molde se transfiere intacto en la cabecera `viz_ext.chart.label_template`.
- Los datos subyacentes (`columns` y el array de filas) permanecen puros, conservando sus llaves originales.

### Ejemplo de Payload (Backend → Frontend)
**Template Solicitado:** `"{{unit_of_measure}} — {{avg_reading_value}} avg"`
```json
{
  "viz_ext": {
    "type": "scatter",
    "chart": {
      "x_dimension": "unit_of_measure",
      "y_dimensions": ["avg_reading_value"],
      "label_template": "{{unit_of_measure}} — {{avg_reading_value}} avg"
    }
  },
  "columns": [
    "unit_of_measure",
    "avg_reading_value"
  ],
  "sample_rows": [
    { "unit_of_measure": "CEL", "avg_reading_value": 59.12 },
    { "unit_of_measure": "KWH", "avg_reading_value": 56.56 }
  ]
}
```

### Instrucción para Frontend
Debes interceptar el evento de *Tooltip/Hover* en el canvas de ECharts (o la biblioteca que utilices) e implementar un reemplazo tipo **Mustache** o **Regex**.

**Ejemplo en JavaScript/TypeScript para ECharts Tooltip Formatter:**
```javascript
tooltip: {
  formatter: function (params) {
    // 1. Extraer el template proporcionado por Janus
    let template = viz_ext.chart.label_template; // "{{unit_of_measure}} — {{avg_reading_value}} avg"
    
    // 2. Extraer la fila actual bajo el puntero
    const row = params.data; // { unit_of_measure: "CEL", avg_reading_value: 59.12 }
    
    // 3. Interpolar
    for (const [key, value] of Object.entries(row)) {
      const regex = new RegExp(`{{${key}}}`, 'g');
      template = template.replace(regex, value);
    }
    
    // 4. Renderizar: "CEL — 59.12 avg"
    return template;
  }
}
```

---

## ⚠️ Advertencia de Alineación de Llaves
Cuando se configuran métricas agregadas (e.g. `SUM`, `AVG`, `COUNT`), el backend autogenera los alias anteponiendo la operación al nombre de la métrica (ej: `avg_reading_value`).

**Regla de Oro:** El `label_template` enviado en el request gRPC **debe** mapear el nombre final de la columna evaluada. 
- ❌ **Incorrecto:** `"Valor: {{reading_value}}"` (Si está agregada, la llave `reading_value` no existirá).
- ✅ **Correcto:** `"Valor: {{avg_reading_value}}"`
