//! Zonas físicas especializadas del formato binario Metri Vortex (`.mbf`).

/// Zona 5: Sinopsis matemáticas `MbfHyperSynopsis512` de 512 bytes para momentos de orden superior, Weibull, econometría y FFT/PSD en O(1).
pub mod bi_synopses;
/// Zona 6B: Matriz de covarianza cruzada multivariante para correlación de Pearson, regresión OLS múltiple, PCA y distancia de Mahalanobis en O(1).
pub mod cross_covariance;
/// Zona 2: Grafo bidireccional CSR y árbol de intervalos Euler Tour jerárquico para rollups de subárbol en O(1).
pub mod csr_graph;
/// Zona 4: Contabilidad de punto fijo exacta al centavo (`FixedDecimal128`), valoración por lotes (FIFO/LIFO/WAC/FEFO), tabla FX multimoneda y cadena Merkle SHA-256.
pub mod financial_ledger;
/// Catálogo agnóstico al dominio de matemáticas difusas, análisis de supervivencia, cadenas de Markov discretas, optimización estocástica y normalización dimensional afín.
pub mod fuzzy_math_catalog;
/// Zona 5B: Motor de búsqueda híbrida en 6 capas (autómata Levenshtein, SymSpell O(1), N-gram BM25F, fonética romance/doble, vectores RaBitQ e índice espacial H3/Morton).
pub mod hybrid_search;
/// Zona 3: Series temporales de alta frecuencia con compresión Delta-of-Delta empaquetada en bits, tramas Chimp128 + LZ4 y pirámides multirresolución (`1m`, `1h`, `1d`).
pub mod iot_timeseries;
/// Zona 1: Almacenamiento columnar PAX con vectores de visibilidad MVCC (*time-travel*), directorio Elias-Fano + MPHF y compresión de símbolos FSST.
pub mod pax_mvcc;
