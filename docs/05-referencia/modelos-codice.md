# Catálogo de Modelos del Códice (`config/models/`)

> Generado automáticamente desde `config/models/*.json` — registro SSOT de esquemas.
> Regenerar: `python3 scripts/docs/gen_reference.py`

Total de modelos registrados: **60**

| Entidad | Motor | Sistema | Historial | Constraints | Reglas EDA | Atributos |
|---|---|:---:|:---:|:---:|:---:|---|
| `agent_metric` | `mbf` | 🔒 | — | 0 | 0 | `tenant_id`, `model`, `request_latency_ms`, `router_latency_ms`, `avg_llm_latency_ms`, `avg_tool_latency_ms`, `iterations`, `input_tokens`, `output_tokens`, `total_tools_called`, `fallback_used`, `modules_selected` |
| `api_key` | `mbf` | 🔒 | ✓ | 0 | 0 | `name`, `tenant_id`, `role_id`, `secret_token_hash`, `expires_at`, `is_active` |
| `approval_instance` | `mbf` | 🔒 | ✓ | 0 | 0 | `policy_id`, `target_entity_name`, `target_entity_id`, `canonical_state`, `sub_state`, `current_tier_level`, `idempotency_hash`, `sla_due_date` |
| `approval_policy` | `mbf` | 🔒 | ✓ | 0 | 0 | `policy_code`, `target_entity_name`, `trigger_condition_ast`, `min_required_tiers`, `tier_configurations`, `resolution_actions`, `trigger_event`, `sla_duration_hours`, `is_active` |
| `approval_step_execution` | `mbf` | 🔒 | ✓ | 0 | 0 | `approval_instance_id`, `tier_level`, `actor_user_id`, `decision`, `rejection_reason_code`, `rejection_comment`, `electronic_signature_id`, `evaluated_at` |
| `asset` | `mbf` | — | ✓ | 0 | 3 | `name`, `tag`, `serial_number`, `barcode`, `status`, `criticality`, `location_id`, `parent_asset_id`, `description`, `manufacturer_company_id`, `model_number`, `vendor_provider_id`, `omniclass_code`, `omniclass_name`, `purchase_date`, `installation_date`, `warranty_expiration_date`, `purchase_cost_cents`, `salvage_value_cents`, `currency`, `cost_center`, `expected_lifespan_months`, `custom_attributes` |
| `audit_log` | `mbf` | — | — | 0 | 0 | `tenant_id`, `user_id`, `action_type`, `resource_domain`, `resource_id`, `client_ip`, `security_context`, `execution_time_ms`, `plugin_telemetry` |
| `calendar_event` | `mbf` | 🔒 | — | 0 | 0 | `source_entity_id`, `source_entity_type`, `start_date`, `end_date`, `display_title`, `cron_expression`, `iana_timezone`, `color_hex` |
| `checklist_template` | `mbf` | — | ✓ | 0 | 0 | `name`, `description`, `status` |
| `checklist_template_item` | `mbf` | — | ✓ | 0 | 0 | `checklist_template_section_id`, `item_order`, `question_text`, `type`, `is_required` |
| `checklist_template_section` | `mbf` | — | ✓ | 0 | 0 | `checklist_template_id`, `section_order`, `title`, `description` |
| `company` | `mbf` | — | ✓ | 0 | 2 | `name`, `legal_name`, `tag`, `company_type`, `parent_company_id`, `tax_id_type`, `tax_id`, `tax_regime`, `status`, `website`, `primary_contact_name`, `primary_contact_role`, `contact_email`, `contact_phone`, `address_street`, `address_city`, `address_state_province`, `address_postal_code`, `address_country_iso2`, `hourly_rate_cents`, `currency`, `payment_terms`, `insurance_expiration_date`, `sla_rating`, `erp_external_id`, `custom_attributes` |
| `dashboardBI` | `mbf` | — | ✓ | 0 | 3 | `name`, `description`, `widgets`, `created_at`, `updated_at` |
| `document_chunk` | `mbf` | 🔒 | — | 0 | 0 | `id`, `parent_file`, `chunk_index`, `chunk_content`, `semantic_embedding`, `metadata_tags`, `owner_entity_type`, `owner_entity_id`, `page_range`, `created_at` |
| `domain_fault` | `mbf` | 🔒 | — | 0 | 0 | `trace_id`, `tenant_id`, `user_id`, `error_code`, `severity`, `stage`, `component`, `entity_type`, `retryable`, `occurred_at`, `context` |
| `domain_plugin` | `mbf` | 🔒 | ✓ | 0 | 0 | `target_domain`, `hook_type`, `handler_fn`, `active`, `config` |
| `domain_quota` | `mbf` | 🔒 | ✓ | 0 | 0 | `tenant_id`, `resource_domain`, `limit_type`, `reset_strategy`, `period_key`, `max_limit`, `current_usage` |
| `downtime_log` | `mbf` | — | ✓ | 0 | 0 | `asset_id`, `work_order_id`, `start_time`, `end_time`, `duration_minutes`, `reason` |
| `electronic_signature` | `mbf` | 🔒 | ✓ | 0 | 0 | `signer_user_id`, `signature_intent`, `timestamp`, `ip_address`, `device_fingerprint`, `snapshot_hash`, `graphical_file_id`, `requires_mfa_token`, `signed_entity`, `signed_entity_id` |
| `event_routing_rule` | `mbf` | 🔒 | ✓ | 0 | 0 | `rule_code`, `description`, `is_system_seeded`, `target_entity_name`, `event_trigger_type`, `filter_conditions`, `detail_type_output` |
| `file` | `mbf` | 🔒 | — | 0 | 0 | `file_name`, `mime_type`, `file_extension`, `file_size_bytes`, `checksum_sha256`, `owner_entity_type`, `owner_entity_id`, `url`, `optimized_url`, `thumbnail_url`, `optimized_size_bytes`, `optimized_mime_type`, `size_reduction_percent`, `processing_status`, `processing_error`, `embed_requested`, `embed_status`, `chunks_count`, `uploaded_by`, `created_at`, `processed_at`, `direct_access_only` |
| `inventory_batch` | `mbf` | — | ✓ | 0 | 0 | `part_id`, `location_id`, `received_quantity`, `remaining_quantity`, `unit_cost_cents`, `currency`, `received_at`, `tenant_id` |
| `inventory_ledger` | `mbf` | 🔒 | — | 0 | 0 | `movement_id`, `part_id`, `location_id`, `timestamp`, `quantity_change`, `running_balance`, `running_financial_value` |
| `inventory_movement` | `mbf` | — | — | 0 | 0 | `part_id`, `location_id`, `type`, `quantity`, `unit_cost_cents`, `currency`, `inventory_batch_id`, `reference_entity`, `reference_id`, `timestamp`, `performed_by`, `tenant_id` |
| `inventory_transfer` | `mbf` | — | ✓ | 0 | 0 | `part_id`, `from_location_id`, `to_location_id`, `quantity`, `status`, `requested_by`, `dispatched_at`, `received_at` |
| `iot_alert_rule` | `mbf` | — | ✓ | 0 | 3 | `asset_id`, `metric_code`, `threshold_operator`, `threshold_value`, `unit_of_measure`, `cooldown_seconds`, `alert_severity`, `notify_users`, `notify_groups`, `is_multi_asset`, `target_asset_id`, `location_id`, `evaluate_on_bad_quality`, `correlation_id` |
| `iot_device_command` | `mbf` | — | ✓ | 0 | 2 | `asset_id`, `command_type`, `protocol`, `payload`, `status`, `response_payload`, `expires_at` |
| `iot_device_profile` | `mbf` | — | ✓ | 0 | 0 | `name`, `inbound_metric_rules`, `inbound_metadata_rules`, `outbound_command_templates` |
| `iot_harvester_config` | `mbf` | — | ✓ | 0 | 0 | `asset_id`, `device_profile_id`, `target_url`, `port`, `polling_interval_seconds`, `schedule_type`, `cron_expression`, `auth_type`, `credentials`, `payload_format`, `http_method`, `http_headers`, `request_payload`, `url_query_params`, `mapping_directive` |
| `iot_subscription` | `mbf` | — | ✓ | 0 | 3 | `asset_id`, `device_profile_id`, `aws_thing_name`, `topic_pattern`, `subscription_status`, `batch_window_seconds` |
| `labor_log` | `mbf` | — | ✓ | 1 | 0 | `work_order_id`, `user_id`, `start_time`, `end_time`, `duration_minutes`, `hourly_rate_cents`, `start_latitude`, `start_longitude`, `end_latitude`, `end_longitude`, `is_geofence_verified` |
| `location` | `mbf` | — | ✓ | 0 | 2 | `name`, `tag`, `floc_code`, `status`, `criticality`, `risk_probability_score`, `risk_impact_score`, `parent_location_id`, `description`, `omniclass_code`, `omniclass_name`, `is_primary`, `latitude`, `longitude`, `address_street`, `address_city`, `address_state_province`, `address_postal_code`, `address_country_iso2`, `area_value`, `area_unit`, `cost_center`, `primary_contact_user_id`, `custom_attributes` |
| `meter_reading` | `mbf` | — | — | 0 | 0 | `asset_id`, `location_id`, `iot_subscription_id`, `device_profile_id`, `aws_thing_name`, `metric_code`, `reading_value`, `raw_value`, `unit_of_measure`, `terminology_system`, `data_quality`, `protocol`, `source_address`, `timestamp`, `ingested_at`, `metadata` |
| `note` | `mbf` | — | ✓ | 0 | 0 | `content`, `author_id`, `timestamp`, `work_order_id` |
| `outbox_event` | `mbf` | 🔒 | — | 0 | 0 | `status`, `detail_type`, `payload`, `retry_count`, `retry_at`, `claimed_at`, `created_at` |
| `part` | `mbf` | — | ✓ | 0 | 0 | `name`, `sku`, `barcode`, `description`, `category`, `default_unit_cost_cents`, `min_quantity`, `uom`, `currency` |
| `preventive_maintenance` | `mbf` | — | ✓ | 1 | 3 | `asset_id`, `location_id`, `title`, `description`, `priority`, `status`, `estimated_duration_hours`, `assignees`, `assigned_group_ids`, `category`, `require_location_verification`, `latitude`, `longitude`, `custom_attributes`, `recurrence_start_date`, `recurrence_end_date`, `recurrence_interval`, `recurrence_unit`, `recurrence_at_time`, `cron_expression`, `iana_timezone`, `next_due_date`, `recurrence_basis`, `advance_notice_value`, `advance_notice_unit`, `advance_notice_time`, `advance_notice_weekday`, `inactive_periods`, `prenotify_before_minutes`, `meter_based_trigger` |
| `preventive_maintenance_procedure` | `mbf` | — | ✓ | 0 | 0 | `preventive_maintenance_id`, `procedure_template_id`, `name`, `procedure_order` |
| `procedure_template` | `mbf` | — | ✓ | 0 | 0 | `name`, `description`, `lifecycle_state`, `template_type`, `version`, `document_code`, `custom_logo` |
| `procedure_template_field` | `mbf` | — | ✓ | 0 | 0 | `procedure_template_id`, `parent_field_id`, `label`, `description`, `field_type`, `choices`, `is_required`, `score`, `field_order` |
| `reminder` | `mbf` | — | ✓ | 0 | 3 | `title`, `message`, `target_user_id`, `target_group_id`, `reminder_datetime`, `prenotify_minutes_array`, `iana_timezone`, `status` |
| `request` | `mbf` | — | ✓ | 0 | 0 | `title`, `description`, `requested_by_user`, `requested_by_email`, `form_data`, `status`, `priority`, `asset_id`, `location_id` |
| `role` | `mbf` | 🔒 | ✓ | 0 | 0 | `name`, `description`, `grants`, `allowed_locations`, `allowed_assets`, `tenant_id` |
| `scheduled_job` | `mbf` | 🔒 | ✓ | 0 | 3 | `parent_entity_ref`, `created_by`, `trigger_type`, `trigger_expression`, `iana_timezone`, `action_type`, `target_user_id`, `target_group_id`, `target_role_id`, `target_webhook_id`, `action_payload`, `idempotency_hash`, `status`, `last_run_at`, `run_count`, `last_error` |
| `sequence_registry` | `mbf` | 🔒 | — | 0 | 0 | `tenant_id`, `sequence_code`, `prefix`, `padding_length`, `current_value`, `parent_scope_tag` |
| `shift_pattern` | `mbf` | — | ✓ | 1 | 1 | `name`, `user_id`, `user_group_id`, `grammar`, `cron_expression`, `iana_timezone`, `span_minutes`, `productive_factor`, `reactive_reserve_pct`, `effective_from`, `effective_to`, `status` |
| `task_template` | `mbf` | — | ✓ | 0 | 0 | `name`, `estimated_duration_minutes` |
| `task_template_item` | `mbf` | — | — | 0 | 0 | `task_template_id`, `description` |
| `technical_specification` | `mbf` | — | — | 1 | 0 | `asset_id`, `concept_code`, `concept_name`, `value`, `spec_order` |
| `technician_shift` | `mbf` | — | ✓ | 5 | 3 | `user_id`, `shift_date`, `start_time`, `end_time`, `status`, `shift_pattern_id`, `kind`, `absence_reason`, `completed_by`, `completed_at` |
| `tenant` | `mbf` | 🔒 | ✓ | 0 | 2 | `name`, `tag`, `status`, `tier`, `require_mfa_for_new_users`, `mfa_policy`, `mfa_allowed_methods`, `session_max_idle_minutes`, `industry`, `timezone`, `currency`, `language`, `billing_admin_email`, `logo`, `config` |
| `tenant_plugin` | `mbf` | 🔒 | ✓ | 0 | 0 | `tenant_id`, `plugin_id`, `status`, `config` |
| `user` | `mbf` | 🔒 | ✓ | 0 | 2 | `username`, `password_hash`, `email`, `primary_phone`, `first_name`, `last_name`, `job_title`, `avatar`, `badge_id`, `status`, `user_type`, `role_ids`, `group_ids`, `tenant_id`, `company_id`, `primary_location_id`, `hourly_rate_cents`, `currency`, `skills`, `timezone`, `locale`, `failed_attempts`, `locked_until`, `mfa_enabled`, `mfa_secret`, `registration_method`, `registered_at`, `last_login_at` |
| `user_group` | `mbf` | — | ✓ | 0 | 0 | `name`, `description`, `allowed_locations`, `allowed_assets`, `parent_user_group_id`, `time_restrictions` |
| `webhook_endpoint` | `mbf` | 🔒 | ✓ | 0 | 0 | `name`, `target_url`, `http_method`, `authentication_type`, `auth_token`, `subscribed_rule_ids`, `max_retries`, `is_active` |
| `work_order` | `mbf` | — | ✓ | 2 | 2 | `work_order_number`, `title`, `description`, `request_id`, `asset_id`, `location_id`, `require_location_verification`, `latitude`, `longitude`, `assignees`, `assigned_group_ids`, `category`, `status`, `priority`, `due_date`, `scheduled_start`, `estimated_duration_hours`, `preventive_maintenance_id`, `scheduled_job_id`, `completed_by`, `completed_at`, `custom_attributes` |
| `work_order_procedure` | `mbf` | — | ✓ | 4 | 0 | `work_order_id`, `procedure_template_id`, `name`, `status`, `score`, `max_score`, `completed_by`, `completed_at`, `procedure_order` |
| `work_order_procedure_field` | `mbf` | — | ✓ | 0 | 0 | `work_order_procedure_id`, `procedure_template_field_id`, `parent_field_id`, `label`, `description`, `field_type`, `choices`, `is_required`, `field_order`, `score`, `max_score`, `value_text`, `value_number`, `value_boolean`, `value_epoch`, `value_choice`, `answered_by`, `answered_at` |
| `work_order_task` | `mbf` | — | — | 0 | 0 | `work_order_id`, `task_template_id`, `description`, `status`, `asset_id`, `location_id`, `assigned_user_id`, `assigned_group_id`, `scheduled_start`, `scheduled_end`, `estimated_duration_minutes`, `completed_by`, `completed_at` |
| `work_order_task_item` | `mbf` | — | — | 0 | 0 | `work_order_task_id`, `task_template_item_id`, `description`, `is_completed` |
