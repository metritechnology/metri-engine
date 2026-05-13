(ns metri.aegis.datalog.sort
  "Ordenación en memoria: SortDefinition AST IR → rows ordenados.
   SRP: sort post-ejecución — Datahike no tiene ORDER BY nativo.

   SortDefinition (§4 contrato):
     :field      → nombre del atributo a ordenar (string o keyword)
     :descending → true = DESC, false/nil = ASC")

(defn sort-oltp-result
  "Ordena un vector de rows (mapas pull Datahike) según vector de SortDefinition.
   Aplicación multi-clave: primera definición tiene mayor prioridad.
   Retorna el mismo vector si sort-defs está vacío."
  [rows sort-defs]
  (if (empty? sort-defs)
    rows
    (let [comparator
          (fn [a b]
            (reduce (fn [acc {:keys [field descending]}]
                      (if (not= acc 0)
                        acc
                        (let [ka  (if (keyword? field) field (keyword field))
                              va  (get a ka)
                              vb  (get b ka)
                              cmp (compare va vb)]
                          (if descending (- cmp) cmp))))
                    0
                    sort-defs))]
      (vec (sort comparator rows)))))
