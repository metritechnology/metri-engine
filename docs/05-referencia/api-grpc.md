# Referencia de Contratos gRPC (`proto/`)

> Generado automáticamente desde `proto/metri.proto` y `proto/eda.proto` — fuente única de verdad (SSOT).
> Regenerar: `python3 scripts/docs/gen_reference.py`

## Archivo `proto/metri.proto`

### Servicio `MetriService`

- `MetriService.Discovery(DiscoveryRequest) -> DiscoveryResponse`
- `MetriService.Explore(ExploreRequest) -> ExploreResponse`
- `MetriService.ListEntities(ListEntitiesRequest) -> ListEntitiesResponse`
- `MetriService.Query(QueryRequest) -> stream QueryResponse`
- `MetriService.Transact(TransactionRequest) -> TransactionResponse`
- `MetriService.CompositeTransact(CompositeTransactRequest) -> CompositeTransactResponse`
- `MetriService.BulkIngest(BulkRequest) -> BulkResponse`
- `MetriService.MatchRoutingRulesBatch(MatchRoutingRulesBatchRequest) -> MatchRoutingRulesBatchResponse`
- `MetriService.ListSequenceConfigs(ListSequenceConfigsRequest) -> ListSequenceConfigsResponse`
- `MetriService.UpsertSequenceConfig(UpsertSequenceConfigRequest) -> UpsertSequenceConfigResponse`

#### Mensaje `Status`

#### Mensaje `Link`

#### Mensaje `Action`

#### Mensaje `Pagination`

#### Mensaje `DiscoveryRequest`

#### Mensaje `AttributeSchema`

#### Mensaje `EntitySchema`

#### Mensaje `DiscoveryResponse`

#### Mensaje `ExploreRequest`

#### Mensaje `ExploreResponse`

#### Mensaje `ListEntitiesRequest`

#### Mensaje `ListEntitiesResponse`

#### Enum `AggregationFunction`

#### Mensaje `MetricDefinition`

#### Mensaje `DimensionDefinition`

#### Mensaje `FormulaEntry`

#### Mensaje `SemanticMetricRef`

#### Mensaje `StringList`

#### Mensaje `FilterValueList`

#### Mensaje `FilterValue`

#### Enum `FilterOperator`

#### Mensaje `FilterCriteria`

#### Mensaje `FilterGroup`

#### Enum `Conjunction`

#### Mensaje `FilterNode`

#### Mensaje `SortDefinition`

#### Mensaje `TimeFrameContext`

#### Enum `TimeFilterType`

#### Mensaje `AnalyticalComparison`

#### Enum `ComparisonType`

#### Enum `ShiftShortcut`

#### Mensaje `HierarchyContext`

#### Enum `OutputCastType`

#### Mensaje `AnalyticsRequest`

#### Mensaje `DashboardCrossFilterContext`

#### Mensaje `QueryMetadata`

#### Mensaje `ColumnSchema`

#### Mensaje `DataRow`

#### Mensaje `DataRowList`

#### Mensaje `RowSet`

#### Mensaje `MultiSeriesGroup`

#### Mensaje `QueryRequest`

#### Mensaje `ChronosAlertTask`

#### Mensaje `QueryResponse`

#### Mensaje `BatchContext`

#### Mensaje `IntelligenceSignal`

#### Mensaje `AnalyticalSignal`

#### Mensaje `TableColumn`

#### Enum `Alignment`

#### Mensaje `TableMeta`

#### Mensaje `IndicatorThreshold`

#### Mensaje `ChartDecoration`

#### Mensaje `BreakdownSignal`

#### Mensaje `TreeMeta`

#### Mensaje `VizMeta`

#### Enum `OperationAction`

#### Mensaje `TransactionRequest`

#### Mensaje `TransactionResponse`

#### Mensaje `CompositeTransactRequest`

#### Mensaje `EntityWriteResult`

#### Mensaje `CompositeTransactResponse`

#### Mensaje `BulkRequest`

#### Mensaje `BulkResponse`

#### Mensaje `MatchRoutingRulesRequest`

#### Mensaje `WebhookTarget`

#### Mensaje `MatchedRule`

#### Mensaje `MatchRoutingRulesResponse`

#### Mensaje `MatchRoutingRulesBatchRequest`

#### Mensaje `MatchRoutingRulesBatchResponse`
### Servicio `QuotaService`

- `QuotaService.ReserveTokens(ReserveTokensRequest) -> ReserveTokensResponse`
- `QuotaService.ReconcileTokens(ReconcileTokensRequest) -> ReconcileTokensResponse`

#### Mensaje `ReserveTokensRequest`

#### Mensaje `ReserveTokensResponse`

#### Mensaje `ReconcileTokensRequest`

#### Mensaje `ReconcileTokensResponse`
### Servicio `AgentConfigService`

- `AgentConfigService.GetAgentConfig(AgentConfigRequest) -> AgentConfigResponse`

#### Mensaje `AgentConfigRequest`

#### Mensaje `AgentConfigResponse`

#### Mensaje `AgentModuleConfig`

#### Mensaje `NavigationRouteConfig`

#### Mensaje `SequenceDefaults`

#### Mensaje `SequenceCounter`

#### Mensaje `ListSequenceConfigsRequest`

#### Mensaje `ListSequenceConfigsResponse`

#### Mensaje `UpsertSequenceConfigRequest`

#### Mensaje `UpsertSequenceConfigResponse`

## Archivo `proto/eda.proto`

### Servicio `MoiraRoutingService`

- `MoiraRoutingService.MatchRoutingRulesBatch`
- `MoiraRoutingService.ResetOrphanedEvents`

#### Mensaje `MatchRoutingRulesBatchRequest`

#### Mensaje `MatchRoutingRulesBatchResponse`

#### Mensaje `ResetOrphanedEventsRequest`

#### Mensaje `ResetOrphanedEventsResponse`
