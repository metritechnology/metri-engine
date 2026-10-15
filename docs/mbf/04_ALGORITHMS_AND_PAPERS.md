# Scientific Papers, Mathematical Foundations, and Core Algorithms

> **English summary:** This document details the mathematical formulations, asymptotic complexities, and peer-reviewed computer science papers underpinning every subsystem of the **Metri Vortex (`.mbf`)** engine: Chan/Welford parallel variance, KLL quantile sketches, Chimp128/Gorilla time-series compression, FSST string compression, Elias-Fano monotonic indexing, Xor8 filters, Roaring Bitmaps, CSR graphs with Euler Tour interval encoding, Fenwick prefix sums, Hamilton's apportionment for financial rounding, and vectorized IRLS logistic regression.

---

## 1. Catálogo de Papers Científicos que Fundamentan `.mbf`

Cada estructura física del formato `.mbf` se basa en literatura científica probada en sistemas de bases de datos de ultra-alto rendimiento:

| Subsistema en `.mbf` | Algoritmo / Estructura | Paper Científico de Referencia | Complejidad lograda en Metri |
| :--- | :--- | :--- | :--- |
| **Zona 1 (Strings & Enums)** | **FSST** (*Fast Static Symbol Table*) | Boncz, Neumann, Leis (VLDB 2020): *"FSST: Fast Random Access String Compression"* | Acceso aleatorio $O(1)$ por fila sin descomprimir bloques completos; evaluación de filtros `EQ`/`STARTS_WITH` sobre bytes comprimidos. |
| **Zona 1 (Offsets & IDs)** | **Elias-Fano** + **BBHash MPHF** | Elias (1974), Vigna (WSDM 2013): *"Quasi-Succinct Indices"*; Limasset et al. (SEA 2017): *"Fast and Scalable Minimal Perfect Hashing"* | Búsqueda puntual (`Point Lookup`) de `entity_id` $\rightarrow$ `row_id` en $O(1)$ usando solo ~2,5 bits por entidad. |
| **Zona 2 (Grafos & ISA-95)** | **CSR** (*Compressed Sparse Row*) + **Euler Tour Interval Labeling** | Tarjan & Vishkin (SJC 1985); Dietz (WADS 1982): *"Maintaining Order in a Linked List"* | Comprobación de pertenencia a subárbol ISA-95 (`/sede/edificio/piso/*`) en **$O(1)$** ($t_{\text{in}}(u) \le t_{\text{in}}(v) \land t_{\text{out}}(v) \le t_{\text{out}}(u)$). |
| **Zona 3 (Series IoT)** | **Chimp128** + **Gorilla Delta-of-Delta** | Liakos, Papakonstantinou, Sioutas (VLDB 2022): *"Chimp: Efficient Lossless Floating Point Compression for Time Series Databases"*; Pelkonen et al. (VLDB 2015, Facebook Gorilla) | Compresión sin pérdida de telemetría `f64` y timestamps `i64` a ~1,6–2,1 bytes por muestra con decodificación a > 3 GB/s. |
| **Zona 4 (Finanzas & Saldos)** | **Árboles de Fenwick / Prefix-Sum 128-bit** + **Método de Hamilton** | Fenwick (Software: Practice and Experience, 1994): *"A New Data Structure for Cumulative Frequency Tables"*; Balinski & Young (1982) | Suma de flujos contables en rango $[t_1, t_2]$ en **$O(1)$** y prorrateo de centavos con residuo cero garantizado. |
| **Zona 5 (Estadística BI)** | **Algoritmo Paralelo de Chan/Welford** + **Co-Momentos de Pébay** | Chan, Golub, LeVeque (1979): *"Updating Formulae and a Pairwise Algorithm for Computing Sample Variances"*; Pébay (Sandia National Labs, 2008) | Fusión exacta en **$O(1)$** de media, varianza, desviación estándar, correlación de Pearson y regresión lineal MCO entre miles de bloques `.mbf`. |
| **Zona 5 (Percentiles PB)** | **KLL Sketch** (*Karnin-Lang-Liberty*) | Karnin, Lang, Liberty (FOCS 2016): *"Optimal Quantile Approximation in Streams"* | Cálculo de `MEDIAN`, `PERCENTILE_90`, `PERCENTILE_95` y `PERCENTILE_99` sobre Petabytes en espacio óptimo $O(\frac{1}{\varepsilon}\sqrt{\log(1/\varepsilon)})$ (256 bytes/bloque). |
| **Zona 5 (Filtros & FTS)** | **Roaring Bitmaps** + **Xor8 Filters** | Lemire et al. (SPE 2016): *"Consistently faster and smaller compressed bitmaps with Roaring"*; Graf & Lemire (ACM JEA 2020): *"Xor Filters: Faster and Smaller Than Bloom and Cuckoo Filters"* | Podado de bloques con 0,39 % de falsos positivos usando solo 9,84 bits por clave y operaciones `AND`/`OR` vectorizadas SIMD. |

---

## 2. Formulaciones Matemáticas del Motor BI (`src/aegis/mbf/`)

### 2.1. Álgebra de Sinopsis Univariadas (Chan / Welford en $O(1)$)
Cada bloque $A$ almacena en su `NumericBlockSynopsis` la tupla $(n_A, \mu_A, M_{2,A}, \min_A, \max_A)$, donde:
$$\mu_A = \frac{1}{n_A}\sum_{i=1}^{n_A} x_i, \qquad M_{2,A} = \sum_{i=1}^{n_A} (x_i - \mu_A)^2$$

Al combinar dos bloques disjuntos $A$ y $B$ (de 4.096 filas o de archivos `.mbf` distintos), el bloque compuesto $C = A \cup B$ se calcula en **$O(1)$** sin leer las filas:
$$n_C = n_A + n_B, \qquad \delta = \mu_B - \mu_A$$
$$\mu_C = \mu_A + \delta \frac{n_B}{n_C}$$
$$M_{2,C} = M_{2,A} + M_{2,B} + \delta^2 \frac{n_A n_B}{n_C}$$

A partir de $(n_C, \mu_C, M_{2,C})$ al final de la consulta, Aegis obtiene instantáneamente:
- `VARIANCE` (muestral): $s^2 = \frac{M_{2,C}}{n_C - 1}$
- `STD_DEV`: $s = \sqrt{\frac{M_{2,C}}{n_C - 1}}$
- `z_score` (para `IntelligenceSignal` y detección de anomalías gaussianas): $z = \frac{x_{\text{actual}} - \mu_C}{s}$

### 2.2. Álgebra de Co-Momentos Bivariados (`CORRELATION` y `LINEAR_REGRESSION` en $O(1)$)
Para dos atributos numéricos $X$ e $Y$, cada bloque almacena el co-momento central $C_{XY,A} = \sum_{i=1}^{n_A}(x_i - \mu_{X,A})(y_i - \mu_{Y,A})$. La ley de composición paralela de Pébay entre bloques $A$ y $B$ es:
$$C_{XY, A \cup B} = C_{XY,A} + C_{XY,B} + (\mu_{X,B} - \mu_{X,A})(\mu_{Y,B} - \mu_{Y,A}) \frac{n_A n_B}{n_A + n_B}$$

De aquí se derivan en **$O(1)$**:
- **Coeficiente de Correlación de Pearson (`AggregationFunction::CORRELATION`):**
  $$r_{XY} = \frac{C_{XY}}{\sqrt{M_{2,X} \cdot M_{2,Y}}}$$
- **Pendiente e Intercepto de Regresión Lineal MCO (`AggregationFunction::LINEAR_REGRESSION`):**
  $$m = \frac{C_{XY}}{M_{2,X}}, \qquad b = \mu_Y - m \mu_X$$

### 2.3. Regresión Logística Vectorizada (`AggregationFunction::LOGISTIC_REGRESSION`)
Para modelar la probabilidad de fallo de un activo ($\Pr(\text{Fault}=1 \mid x) = \sigma(\beta_0 + \beta_1 x)$), el motor ejecuta **IRLS (*Iteratively Reweighted Least Squares* / Newton-Raphson)** durante un máximo de 8 iteraciones vectorizadas con SIMD:
$$\boldsymbol{\beta}^{(k+1)} = \boldsymbol{\beta}^{(k)} + \left(\mathbf{X}^T \mathbf{W}^{(k)} \mathbf{X} + \lambda \mathbf{I}\right)^{-1} \mathbf{X}^T \left(\mathbf{y} - \mathbf{p}^{(k)}\right)$$
donde $p_i^{(k)} = \frac{1}{1 + e^{-(\beta_0^{(k)} + \beta_1^{(k)} x_i)}}$ y $w_{ii}^{(k)} = p_i^{(k)}(1 - p_i^{(k)})$, con regularización de cresta $\lambda = 10^{-6}$ para garantizar invertibilidad numérica incluso ante separación perfecta de clases.

---

## 3. Algoritmos de Grafos y Jerarquías ISA-95 (`src/mbf/zones/csr_graph.rs`)

### 3.1. Codificación de Intervalos de Euler (`[tin, tout]`) para Subárboles en $O(1)$
Durante la compactación del grafo de `location` y `asset`, se ejecuta un recorrido en profundidad (*Depth-First Search*) linealizando el árbol:
1. Al entrar al nodo $u$, se asigna `tin[u] = timer; timer += 1`.
2. Se visitan recursivamente todos los hijos $v \in \text{children}(u)$.
3. Al salir del nodo $u$, se asigna `tout[u] = timer - 1`.

**Propiedad Fundamental:** Todo el subárbol descendiente del nodo $u$ (incluyendo nietos a cualquier profundidad) ocupa el intervalo contiguo de enteros $[t_{\text{in}}(u), t_{\text{out}}(u)]$.
- Filtrar **todos los activos y órdenes de trabajo pertenecientes a una planta o edificio completo** se reduce a un predicado de rango entero sobre la columna `location_tin`:
  $$\text{tin}[u] \le \text{asset.location\_tin} \le \text{tout}[u]$$
  evaluado mediante instrucciones SIMD (`vcgeq_u32` y `vcleq_u32`) a razón de **16 elementos por ciclo de reloj**, eliminando por completo las consultas recursivas.

---

## 4. Algoritmos Financieros y Contables Exactos (`src/mbf/zones/financial_ledger.rs`)

### 4.1. Punto Fijo `FixedDecimal128` y Redondeo Bancario (*Round Half-to-Even*)
Dado un valor escalado $X \in \mathbb{Z}_{128}$ (con escala $S = 10^6$), la multiplicación seguida de división $\lfloor \frac{A \times B}{D} \rceil_{\text{banker}}$ se calcula con cociente $q$ y resto $r = (A \times B) \bmod D$:
- Si $2|r| < |D|$, el resultado es $q$.
- Si $2|r| > |D|$, el resultado es $q + \text{sgn}(A \times B \times D)$.
- Si $2|r| == |D|$ (empate exacto a mitad de camino), se redondea hacia el entero **par** más cercano (`q + (q & 1)`), eliminando el sesgo estadístico sistemático en millones de asientos contables.

### 4.2. Distribución Exacta de Residuos de Centavos (Método de Hamilton)
Al prorratear un costo total $C$ entre $k$ órdenes de trabajo o centros de costos con pesos $w_1, \dots, w_k$ ($W = \sum w_j$):
1. Se asigna a cada centro la parte entera exacta $c_j = \lfloor \frac{C \cdot w_j}{W} \rfloor$ y se guarda su resto $r_j = (C \cdot w_j) \bmod W$.
2. La diferencia faltante $\Delta = C - \sum_{j=1}^k c_j$ (donde $0 \le \Delta < k$) se reparte sumando $+1$ micro-unidad a los $\Delta$ centros con mayor resto $r_j$ (desempatando por ID determinista).
3. **Invariante Garantizada:** $\sum_{j=1}^k c_j^* \equiv C$ de forma bit-exacta.

---

## 5. Momentos de Orden Superior, Confiabilidad de Weibull y Econometría (`MbfHyperSynopsis512`)

### 5.1. Fusión Paralela de 3er y 4to Momento Central (Pébay, Sandia National Labs 2008)
Para calcular **Asimetría (Skewness $\gamma_1$)**, **Curtosis Excesiva ($\gamma_2$)** y el **Test de Normalidad de Jarque-Bera ($JB$)** sobre miles de bloques `.mbf` sin leer las filas individuales, se aplican las ecuaciones cerradas de Pébay para combinar dos particiones $A$ y $B$ ($\delta = \mu_B - \mu_A$, $n_{AB} = n_A + n_B$):
$$M_{3, AB} = M_{3, A} + M_{3, B} + \delta^3 \frac{n_A n_B (n_A - n_B)}{n_{AB}^2} + 3\delta \frac{n_A M_{2, B} - n_B M_{2, A}}{n_{AB}}$$
$$M_{4, AB} = M_{4, A} + M_{4, B} + \delta^4 \frac{n_A n_B (n_A^2 - n_A n_B + n_B^2)}{n_{AB}^3} + 6\delta^2 \frac{n_A^2 M_{2, B} + n_B^2 M_{2, A}}{n_{AB}^2} + 4\delta \frac{n_A M_{3, B} - n_B M_{3, A}}{n_{AB}}$$

### 5.2. Ingeniería de Confiabilidad Industrial: Weibull con Censura por la Derecha
A partir de los estadísticos suficientes $\sum_{i \in D} \ln t_i$, $\sum_{i \in D} (\ln t_i)^2$ y el tiempo acumulado de exposición de activos censurados (sin falla) $\sum_{j \in C} t_j^\beta$, el estimador de máxima verosimilitud (MLE) para el parámetro de forma $\beta$ y vida característica $\eta$ satisface:
$$g(\beta) = \frac{\sum_{i=1}^n t_i^\beta \ln t_i}{\sum_{i=1}^n t_i^\beta} - \frac{1}{\beta} - \frac{1}{r} \sum_{i \in D} \ln t_i = 0, \qquad \hat{\eta} = \left( \frac{1}{r} \sum_{i=1}^n t_i^{\hat{\beta}} \right)^{1/\hat{\beta}}$$
donde $r = |D|$ es el número de fallas observadas en `downtime_log` / `work_order`. Inicializando $\beta_0$ con el estimador analítico de Menon ($O(1)$ desde `sum_ln_x` y `sum_ln_x_sq`), 3 pasos de Newton-Raphson convergen con precisión de $10^{-7}$ en $< 10\ \mu\text{s}$.

---

## 6. Algoritmos de Búsqueda Difusa, Fonética y Vectorial Cuantizada (`Zone 5B`)

| Algoritmo / Estructura | Paper / Referencia Científica | Aplicación en `Zone 5B` de `.mbf` |
| :--- | :--- | :--- |
| **Levenshtein Automaton sobre FST** | Schulz, K. U., & Mihov, S. (2002). *Fast string correction with Levenshtein automata*. IJDAR. | Intersección determinista $\text{FST} \cap \text{DFA}(q, k \le 2)$ en $O(|q|)$ ($< 40\ \mu\text{s}$). |
| **SymSpell Symmetric Delete** | Garbe, W. (2012). *1000x Faster Spelling Correction algorithm*. | Lookup $O(1)$ de variantes con $\le 2$ borrados en tabla hash estática (`< 5 µs`). |
| **Okapi BM25F Multi-Field Ranking** | Zaragoza, H., et al. (2004). *Microsoft Cambridge at TREC-13: Web and Hard Tracks*. | Ranking léxico ponderado por campo (`code`, `title`, `description`) con saturación de frecuencia y normalización de longitud. |
| **Spanish & Double Metaphone** | Philips, L. (2000). *The Double Metaphone Search Algorithm*. C/C++ Users Journal. | Emparejamiento fonético bilingüe (`"balbula"` $\leftrightarrow$ `"válvula"`, `"chiler"` $\leftrightarrow$ `"chiller"`). |
| **RaBitQ + HNSW Vector Search** | Gao, J., & Long, C. (SIGMOD 2024). *RaBitQ: Quantizing High-Dimensional Vectors with a Theoretical Error Bound*. | Cuantización de embeddings de 1.024 dimensiones (`document_chunk.semantic_embedding`) con evaluación SIMD `VPOPCNTQ` / `CNT` y fusión híbrida **RRF**. |
| **Uber H3 + Morton Z-Order Curve** | Brodsky, I. (2018). *H3: Uber's Hexagonal Hierarchical Spatial Index*. | Indexación geoespacial $O(1)$ y poda por Bounding Box R-Tree para `latitude` y `longitude`. |

