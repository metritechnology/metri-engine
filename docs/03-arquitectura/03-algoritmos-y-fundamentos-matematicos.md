# Algoritmos y Fundamentos Matemáticos del Motor `.mbf`

> **Propósito:** Documentar con rigor matemático las formulaciones, estructuras de datos sub-lineales y garantías de complejidad temporal y espacial implementadas en las 8 zonas de `src/mbf/zones/`.

---

## 1. Resumen de Complejidades Algorítmicas por Zona

El motor **Metri Vortex (`.mbf`)** reemplaza los escaneos lineales $O(N)$ por estructuras sucintas, árboles de prefijos algebraicos y resúmenes estadísticos fusionables que responden consultas analíticas y de búsqueda en tiempo constante $O(1)$ o logarítmico $O(\log N)$:

| Subsistema / Operación | Estructura en `src/mbf/zones/` | Complejidad Temporal | Complejidad Espacial |
| :--- | :--- | :---: | :---: |
| **Localización de Entidad por ID** | `EliasFanoEntityDirectory` + MPHF `xxh64` (`pax_mvcc.rs`) | $O(1)$ | $\approx 2 + \lceil \log_2(U/n) \rceil$ bits/entidad |
| **Poda de Pertenencia en Bloque** | `Xor8Filter` (`bi_synopses.rs`) | $O(1)$ (3 accesos a memoria) | $1.23 \times 8 \approx 9.84$ bits/llave ($\text{FPR} < 0.39\%$) |
| **Descendencia en Árboles Jerárquicos** | `EulerTreeNode` (`tin`/`tout`) + SIMD 16-lane (`csr_graph.rs`) | $O(1)$ por par / $O(N/16)$ barrido | 16 bytes por nodo |
| **Saldos Contables Acumulados / Rangos** | `FenwickPrefixSum128` sobre `FixedDecimal128` (`financial_ledger.rs`) | $O(\log N)$ | $16 \cdot N$ bytes (`i128`) |
| **Momentos Estadísticos ($M_1..M_4$), Asimetría y Curtosis** | Recurrencia de Pébay en `MbfHyperSynopsis512` (`bi_synopses.rs`) | $O(1)$ por consulta y fusión | 512 bytes por bloque (8 líneas L1) |
| **Búsqueda Tolerante a Errores Tipográficos** | `LevenshteinAutomatonDfa` ($k \le 2$) + `SymSpell` (`hybrid_search.rs`) | $O(1)$ lookup de borrados | Compactado en FST |
| **Similitud Vectorial Semántica** | Cuantización binaria asimétrica `RaBitQVector` + POPCNT (`hybrid_search.rs`) | $O(D / 64)$ instrucciones CPU | 1 bit por dimensión + 8B factores |
| **Regresión Lineal Múltiple (OLS)** | `MbfCrossCovarianceMatrix` + Factorización de Cholesky $L L^T$ (`cross_covariance.rs`) | **$O(M^3)$** (independiente de $N$ filas) | $8 \cdot M^2$ bytes |

---

## 2. Zona 1: Codificación Sucinta Elias-Fano, MPHF y Compresión FSST (`pax_mvcc.rs`)

### 2.1 Directorio Monótono Elias-Fano y Hash Perfecto Mínimo (MPHF)

Para localizar en $O(1)$ la posición física de una entidad dentro de un micro-bloque PAX de $n$ entidades sobre un universo de offsets $U$:

1. Cada `entity_id` se proyecta mediante dos semillas independientes de `xxh64` para construir una tabla de desplazamiento libre de colisiones (**MPHF — Minimal Perfect Hash Function**).
2. La secuencia monótona creciente de offsets $0 \le x_1 \le x_2 \le \dots \le x_n < U$ se divide en:
   - **Bits inferiores (`low_bits`)**: $l = \max\left(0, \lfloor \log_2(U / n) \rfloor\right)$ bits por elemento, empaquetados densamente.
   - **Bits superiores (`high_bits`)**: Codificados en un vector de bits unario donde el $i$-ésimo elemento enciende el bit en la posición $\lfloor x_i / 2^l \rfloor + i$, permitiendo acceso aleatorio mediante operaciones `select1` en $O(1)$.

### 2.2 Visibilidad Temporal MVCC (*Snapshot Isolation*)

Cada versión de fila posee el intervalo semiabierto de épocas $[\text{tx\_assert\_epoch}, \text{tx\_retract\_epoch})$. Para cualquier época de lectura $e_{\text{query}}$, el predicado de visibilidad evaluado de forma vectorizada es:

$$\text{Visible}(row, e_{\text{query}}) \iff \text{tx\_assert\_epoch}(row) \le e_{\text{query}} < \text{tx\_retract\_epoch}(row)$$

---

## 3. Zona 2: Álgebra de Grafos CSR y Recorrido de Euler (`csr_graph.rs`)

Para responder consultas jerárquicas (como consolidación de cuentas contables, estructuras de costos BOM o jerarquías organizacionales) sin recursión ni joins iterativos:

1. Al indexar un árbol jerárquico, el motor ejecuta un recorrido en profundidad (**Euler Tour**) asignando a cada nodo $u$ un tiempo de entrada $\text{tin}(u)$ y un tiempo de salida $\text{tout}(u)$ en `EulerTreeNode` (16 bytes).
2. **Teorema de Contención de Subárbol**: Un nodo $v$ pertenece al subárbol enraizado en $u$ (a cualquier profundidad) si y solo si:

$$\text{tin}(u) \le \text{tin}(v) \le \text{tout}(u)$$

La función `filter_rows_in_subtree_vectorized` evalúa esta desigualdad procesando **16 nodos simultáneamente** por iteración (auto-vectorizable en registros ARM64 NEON), logrando tiempos de filtrado sub-milisegundo sobre decenas de miles de nodos.

---

## 4. Zona 3: Series Temporales Chimp128 y Delta-of-Delta (`iot_timeseries.rs`)

Para flujos de telemetría IoT y métricas continuas (`Chimp128SeriesBlock`):

1. **Normalización Afín Canónica**: Si un sensor reporta en unidades derivadas (ej. Fahrenheit o PSI), se transforma al Sistema Internacional en el ingreso mediante $y = a \cdot x + b$.
2. **Timestamps Delta-of-Delta**: Dados timestamps consecutivos $t_{k-2}, t_{k-1}, t_k$, el motor calcula la segunda diferencia discreta:

$$\Delta_k = t_k - t_{k-1}, \qquad \Delta^2_k = \Delta_k - \Delta_{k-1}$$

Cuando el muestreo es periódico ($\Delta^2_k = 0$), la marca temporal completa de 64 bits se codifica en **exactamente 1 bit (`0`)**.
3. **Compresión de Punto Flotante Chimp128**: Cada valor IEEE-754 de 64 bits ($v_k$) se opera mediante XOR bit a bit contra el mejor candidato dentro de una ventana deslizante de 128 valores previos ($r_k = v_k \oplus v_{\text{best}}$), cuantificando los ceros iniciales (*leading zeros*) y finales (*trailing zeros*) significativos antes de aplicar una pasada final LZ4 (`lz4_flex`).

---

## 5. Zona 4: Aritmética Financiera Exacta, Hamilton y Árbol de Fenwick (`financial_ledger.rs`)

### 5.1 Punto Fijo de 128 Bits (`FixedDecimal128`) y Redondeo Bancario IEEE 754-2008

Toda cifra monetaria se representa internamente como un entero con signo de 128 bits (`i128`) escalado por $S = 10^4$ (`SCALE = 10_000`, con soporte de micro-unidades $10^6$). Ninguna operación financiera utiliza `f64`.

En multiplicaciones y divisiones (`mul_bankers`, `div_bankers`), dado un cociente exacto con parte entera $q$ y residuo $r$ respecto al divisor $d$:
- Si $2r < d$, se trunca a $q$.
- Si $2r > d$, se redondea hacia arriba a $q + \text{sgn}(q)$.
- **Caso de empate exacto ($2r = d$)**: Se aplica **Half-to-Even (Redondeo Bancario)**, sumando $1$ solo si $q$ es impar ($q \bmod 2 \ne 0$), eliminando el sesgo estadístico sistemático en millones de transacciones.

### 5.2 Método del Mayor Resto de Hamilton (`allocate_hamilton`)

Para distribuir un monto total $T$ entre pesos proporcionales $w_1, w_2, \dots, w_k$ garantizando que la suma de las partes sea **exactamente idéntica** al total sin perder ni ganar un diezmilésimo ($\sum_{i=1}^k a_i = T$):

$$q_i = \left\lfloor \frac{T \cdot w_i}{\sum_{j=1}^k w_j} \right\rfloor, \qquad r_i = (T \cdot w_i) \bmod \sum_{j=1}^k w_j$$

Las $R = T - \sum_{i=1}^k q_i$ unidades indivisibles restantes se asignan una a una a las partidas con mayor residuo $r_i$.

### 5.3 Árbol de Fenwick (`FenwickPrefixSum128`) y Cadena Merkle

El arreglo implícito de Fenwick mantiene sumas parciales indexadas por el bit menos significativo ($\text{lsb}(i) = i \ \& \ -i$), permitiendo consultar el saldo acumulado desde el origen hasta cualquier asiento $idx$ (o entre dos fechas $[a, b]$) en **$O(\log N)$**:

$$\text{PrefixSum}(idx) = \sum_{k \text{ por } k \leftarrow k - (k \& -k)} \text{tree}[k]$$

Cada bloque contable sella además un `LedgerAuditHeader` de 64 bytes con un hash **Merkle SHA-256** encadenado al bloque anterior y verifica el invariante de partida doble $\sum \text{Débitos} = \sum \text{Créditos}$.

---

## 6. Zona 5: Hiper-Sinopsis Estadística `MbfHyperSynopsis512` (`bi_synopses.rs`)

Cada bloque numérico almacena una estructura `MbfHyperSynopsis512` de **exactamente 512 bytes (8 líneas de caché L1)** que condensa **32 familias de estadísticos** actualizables en streaming y fusionables en $O(1)$:

1. **Momentos Centrales de Orden Superior (Recurrencia Paralela de Pébay)**: Mantiene la media $\mu$ y las sumas centradas $M_2, M_3, M_4$. Al fusionar dos bloques disjuntos $A$ y $B$ con conteos $n_A, n_B$, diferencia de medias $\delta = \mu_B - \mu_A$ y $n = n_A + n_B$:

$$M_2^{(AB)} = M_2^{(A)} + M_2^{(B)} + \delta^2 \frac{n_A n_B}{n}$$

$$M_3^{(AB)} = M_3^{(A)} + M_3^{(B)} + \delta^3 \frac{n_A n_B (n_A - n_B)}{n^2} + 3\delta \frac{n_A M_2^{(B)} - n_B M_2^{(A)}}{n}$$

De aquí se derivan en $O(1)$ la varianza $\sigma^2 = M_2 / (n-1)$, la **asimetría de Fisher ($\gamma_1$)** y el **exceso de curtosis ($\gamma_2$)**.
2. **Econometría y Series Temporales en 1 Pasada**: Autocorrelación de rezago 1 y 2 ($\text{ACF}_1, \text{ACF}_2$), estadístico **Durbin-Watson** ($DW = \frac{\sum (e_t - e_{t-1})^2}{\sum e_t^2} \in [0, 4]$), tendencia no paramétrica de **Mann-Kendall**, medias móviles exponenciales (**EMA** rápida y lenta) y detector secuencial de cambio de régimen **CUSUM** bilateral ($S_t^+, S_t^-$).
3. **Análisis Espectral y Confiabilidad**: Valor eficaz (**RMS**), factor de cresta, densidad espectral de potencia (**PSD**) en 8 bandas vía filtro de **Goertzel**, estimadores de máxima verosimilitud **Weibull ($\beta, \eta$)**, 48 registros **HyperLogLog++** para cardinalidad `COUNT(DISTINCT)` y 16 cuantiles de cola.

---

## 7. Zona 5B: Búsqueda Híbrida de 6 Capas, RaBitQ y Fusión RRF (`hybrid_search.rs`)

1. **Puntuación Léxica Multicampo BM25F**: Combina frecuencias de términos ponderadas por campo con saturación $k_1 = 1.2$ y normalización de longitud $b = 0.75$.
2. **Cuantización Vectorial Binaria Asimétrica (`RaBitQVector`)**: Proyecta embeddings de alta dimensión en vectores de 1 bit por dimensión centrados en el centroide del clúster. La distancia angular entre el vector de consulta y miles de documentos se evalúa usando instrucciones nativas de conteo de bits (`u64::count_ones` / **POPCNT**):

$$\text{XOR\_Sim}(q, d) = 1 - \frac{2 \cdot \text{popcnt}(q_{\text{bits}} \oplus d_{\text{bits}})}{D}$$

3. **Fusión de Rango Recíproco (`compute_rrf_score`)**: Para combinar sin necesidad de calibración ad-hoc los rankings de la búsqueda léxica/difusa, vectorial semántica y geoespacial (**Uber H3** + Haversine), aplica con $k = 60$:

$$\text{RRF}(d) = \sum_{m \in \{\text{léxico}, \text{vectorial}, \text{espacial}\}} \frac{w_m}{k + \text{rank}_m(d)}$$

---

## 8. Zonas 6A y 6B: Markov, Kaplan-Meier, Benford y Regresión Cholesky en $O(M^3)$

### 8.1 Cadenas de Markov, Supervivencia Kaplan-Meier y Ley de Benford (`fuzzy_math_catalog.rs`)

- **Estimador No Paramétrico de Kaplan-Meier**: Calcula la probabilidad de supervivencia acumulada $\hat{S}(t)$ manejando observaciones con **censura por la derecha** (clientes activos o equipos aún operativos):

$$\hat{S}(t) = \prod_{t_i \le t} \left(1 - \frac{d_i}{n_i}\right)$$

donde $n_i$ es la población en riesgo justo antes de $t_i$ y $d_i$ el número de eventos observados en $t_i$.
- **Auditoría Forense de Ley de Benford**: Compara la distribución empírica del primer dígito significativo $d \in \{1, \dots, 9\}$ de los importes transaccionales contra la probabilidad teórica $P(d) = \log_{10}(1 + 1/d)$ mediante el estadístico $\chi^2$ con 8 grados de libertad (umbral crítico $\chi^2_{0.95, 8} = 15.51$).

### 8.2 Matriz de Covarianza Cruzada y Regresión Múltiple OLS en $O(M^3)$ (`cross_covariance.rs`)

`MbfCrossCovarianceMatrix` mantiene los co-momentos centrados $C_{i,j} = \sum_{k=1}^N (x_{k,i} - \bar{x}_i)(x_{k,j} - \bar{x}_j)$ entre pares de atributos numéricos y se fusiona entre bloques en $O(M^2)$ mediante la generalización bilineal de Pébay:

$$C_{i,j}^{(AB)} = C_{i,j}^{(A)} + C_{i,j}^{(B)} + (\bar{x}_i^{(B)} - \bar{x}_i^{(A)})(\bar{x}_j^{(B)} - \bar{x}_j^{(A)}) \frac{n_A n_B}{n_A + n_B}$$

Gracias a que $X^T X$ y $X^T y$ centrados están pre-computados en la matriz $C_{i,j}$ del bloque:
- El motor resuelve cualquier **Regresión Lineal Múltiple (OLS)** $\hat{\beta} = (X^T X + \lambda I)^{-1} X^T y$ aplicando **Factorización de Cholesky ($A = L L^T$)** seguida de sustitución hacia adelante y hacia atrás (`solve_cholesky`) en tiempo **$O(M^3)$**, **sin leer ni una sola fila individual de la tabla**, obteniendo simultáneamente el coeficiente de determinación $R^2$, la **Primera Componente Principal (PCA)** y la **Distancia Multivariada de Mahalanobis** $D_M(x) = \sqrt{(x - \mu)^T \Sigma^{-1} (x - \mu)}$.
