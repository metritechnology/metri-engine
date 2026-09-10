# Referencia gRPC

> Generado desde `proto/metri.proto` — la fuente única. No editar a mano.
> Regenerar: `python3 scripts/docs/gen_reference.py`

## Servicio `MetriService`

- `MetriService.Discovery`
- `MetriService.Explore`
- `MetriService.ListEntities`
- `MetriService.Query`
- `MetriService.Transact`
- `MetriService.CompositeTransact`
- `MetriService.BulkIngest`
- `MetriService.MatchRoutingRulesBatch`

### Mensaje `Status`

### Mensaje `Link`

### Mensaje `Action`

### Mensaje `Pagination`

### Mensaje `DiscoveryRequest`

### Mensaje `AttributeSchema`

### Mensaje `EntitySchema`

### Mensaje `DiscoveryResponse`

### Mensaje `ExploreRequest`

### Mensaje `ExploreResponse`

### Mensaje `ListEntitiesRequest`

### Mensaje `ListEntitiesResponse`

### Enum `AggregationFunction`

### Mensaje `MetricDefinition`

### Mensaje `DimensionDefinition`

### Mensaje `FormulaEntry`

### Mensaje `SemanticMetricRef`

### Mensaje `StringList`

### Mensaje `FilterValueList`

### Mensaje `FilterValue`

### Enum `FilterOperator`

### Mensaje `FilterCriteria`

### Mensaje `FilterGroup`

### Enum `Conjunction`

### Mensaje `FilterNode`

### Mensaje `SortDefinition`

### Mensaje `TimeFrameContext`

### Enum `TimeFilterType`

### Mensaje `AnalyticalComparison`

### Enum `ComparisonType`

### Enum `ShiftShortcut`

### Mensaje `HierarchyContext`

### Enum `OutputCastType`

### Mensaje `AnalyticsRequest`

### Mensaje `DashboardCrossFilterContext`

### Mensaje `QueryMetadata`

### Mensaje `ColumnSchema`

### Mensaje `DataRow`

### Mensaje `DataRowList`

### Mensaje `RowSet`

### Mensaje `MultiSeriesGroup`

### Mensaje `QueryRequest`

### Mensaje `ChronosAlertTask`

### Mensaje `QueryResponse`

### Mensaje `BatchContext`

### Mensaje `IntelligenceSignal`

### Mensaje `AnalyticalSignal`

### Mensaje `TableColumn`

### Enum `Alignment`

### Mensaje `TableMeta`

### Mensaje `IndicatorThreshold`

### Mensaje `ChartDecoration`

### Mensaje `BreakdownSignal`

### Mensaje `TreeMeta`

### Mensaje `VizMeta`

### Enum `OperationAction`

### Mensaje `TransactionRequest`

### Mensaje `TransactionResponse`

### Mensaje `CompositeTransactRequest`

### Mensaje `EntityWriteResult`

### Mensaje `CompositeTransactResponse`

### Mensaje `BulkRequest`

### Mensaje `BulkResponse`

### Mensaje `MatchRoutingRulesRequest`

### Mensaje `WebhookTarget`

### Mensaje `MatchedRule`

### Mensaje `MatchRoutingRulesResponse`

### Mensaje `MatchRoutingRulesBatchRequest`

### Mensaje `MatchRoutingRulesBatchResponse`
## Servicio `QuotaService`

- `QuotaService.ReserveTokens`
- `QuotaService.ReconcileTokens`

### Mensaje `ReserveTokensRequest`

### Mensaje `ReserveTokensResponse`

### Mensaje `ReconcileTokensRequest`

### Mensaje `ReconcileTokensResponse`
## Servicio `AgentConfigService`

- `AgentConfigService.GetAgentConfig`

### Mensaje `AgentConfigRequest`

### Mensaje `AgentConfigResponse`

### Mensaje `AgentModuleConfig`

### Mensaje `NavigationRouteConfig`
