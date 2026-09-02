# Modelos del Códice

> Generado desde `config/models/*.json` — el registro SSOT de esquemas.
> Regenerar: `python3 scripts/docs/gen_reference.py`

Total de modelos: **60**

| Entidad | Motor | Atributos |
|---|---|---|
| `agent_metric` | oltp | `tenant_id`, `model`, `request_latency_ms`, `router_latency_ms`, `avg_llm_latency_ms`, `avg_tool_latency_ms`, `iterations`, `input_tokens`, `output_tokens`, `total_tools_called`, `fallback_used`, `modules_selected` |
| `api_key` | oltp | `name`, `tenant_id`, `role_id`, `secret_token_hash`, `expires_at`, `is_active` |
| `approval_instance` | oltp | `policy_id`, `target_entity_name`, `target_entity_id`, `canonical_state`, `sub_state`, `current_tier_level`, `idempotency_hash`, `sla_due_date` |
| `approval_policy` | oltp | `policy_code`, `target_entity_name`, `trigger_condition_ast`, `min_required_tiers`, `tier_configurations`, `sla_duration_hours`, `is_active` |
| `approval_step_execution` | oltp | `approval_instance_id`, `tier_level`, `actor_user_id`, `decision`, `rejection_reason_code`, `rejection_comment`, `electronic_signature_id`, `evaluated_at` |
| `asset` | oltp | `name`, `tag`, `serial_number`, `status`, `criticality`, `location_id`, `parent_asset_id`, `category`, `type`, `manufacturer`, `model_number`, `vendor_provider_id`, `omniclass_code`, `omniclass_name`, `purchase_date`, `installation_date`, `warranty_expiration_date`, `purchase_cost_cents`, `salvage_value_cents`, `currency`, `expected_lifespan_months`, `health_score`, `current_meter_reading`, `specifications`, `custom_attributes`, `photo_ids` |
| `audit_log` | olap | `tenant_id`, `user_id`, `action_type`, `resource_domain`, `resource_id`, `client_ip`, `security_context`, `execution_time_ms`, `plugin_telemetry` |
| `calendar_event` | oltp | `source_entity_id`, `source_entity_type`, `start_date`, `end_date`, `display_title`, `cron_expression`, `iana_timezone`, `color_hex` |
| `check_list` | oltp | `work_order_id`, `work_order_task_id`, `form_template_id`, `title`, `description`, `is_completed` |
| `check_list_item` | oltp | `check_list_section_id`, `form_template_field_id`, `question_text`, `type`, `response_boolean`, `response_text`, `response_number`, `response_file_ids`, `response_signature_id`, `observations`, `completed_by`, `completed_at` |
| `check_list_section` | oltp | `check_list_id`, `form_template_section_id`, `section_order`, `title`, `description`, `is_completed` |
| `company` | oltp | `name`, `legal_name`, `tag`, `company_type`, `parent_company_id`, `tax_id`, `tax_regime`, `status`, `website`, `primary_contact_name`, `primary_contact_role`, `contact_email`, `contact_phone`, `address`, `hourly_rate_cents`, `currency`, `payment_terms`, `insurance_expiration_date`, `sla_rating`, `erp_external_id`, `custom_attributes` |
| `dashboardBI` | oltp | `name`, `description`, `widgets`, `created_at`, `updated_at` |
| `document_chunk` | oltp | `id`, `parent_file`, `chunk_index`, `chunk_content`, `semantic_embedding`, `metadata_tags`, `owner_entity_type`, `owner_entity_id`, `page_range`, `created_at` |
| `domain_fault` | olap | `trace_id`, `tenant_id`, `user_id`, `error_code`, `severity`, `stage`, `component`, `entity_type`, `retryable`, `occurred_at`, `context` |
| `domain_plugin` | oltp | `target_domain`, `hook_type`, `clojure_fn`, `active`, `config` |
| `domain_quota` | oltp | `tenant_id`, `resource_domain`, `limit_type`, `reset_strategy`, `period_key`, `max_limit`, `current_usage` |
| `downtime_log` | oltp | `asset_id`, `work_order_id`, `start_time`, `end_time`, `duration_minutes`, `reason` |
| `electronic_signature` | oltp | `signer_user_id`, `signature_intent`, `timestamp`, `ip_address`, `device_fingerprint`, `snapshot_hash`, `graphical_file_id`, `requires_mfa_token`, `signed_entity`, `signed_entity_id` |
| `event_routing_rule` | oltp | `rule_code`, `description`, `is_system_seeded`, `target_entity_name`, `event_trigger_type`, `filter_conditions`, `detail_type_output` |
| `file` | oltp | `file_name`, `mime_type`, `file_extension`, `file_size_bytes`, `checksum_sha256`, `owner_entity_type`, `owner_entity_id`, `url`, `optimized_url`, `thumbnail_url`, `optimized_size_bytes`, `optimized_mime_type`, `size_reduction_percent`, `processing_status`, `processing_error`, `embed_requested`, `embed_status`, `chunks_count`, `uploaded_by`, `created_at`, `processed_at`, `direct_access_only` |
| `form_template` | oltp | `name`, `description`, `version` |
| `form_template_field` | oltp | `form_template_section_id`, `field_order`, `question_prompt`, `type`, `is_required`, `validation_rules` |
| `form_template_section` | oltp | `form_template_id`, `section_order`, `title`, `description` |
| `inventory_batch` | oltp | `part_id`, `location_id`, `received_quantity`, `remaining_quantity`, `unit_cost_cents`, `currency`, `received_at`, `tenant_id` |
| `inventory_ledger` | olap | `movement_id`, `part_id`, `location_id`, `timestamp`, `quantity_change`, `running_balance`, `running_financial_value` |
| `inventory_movement` | oltp | `part_id`, `location_id`, `type`, `quantity`, `unit_cost_cents`, `currency`, `inventory_batch_id`, `reference_entity`, `reference_id`, `timestamp`, `performed_by`, `tenant_id` |
| `inventory_transfer` | oltp | `part_id`, `from_location_id`, `to_location_id`, `quantity`, `status`, `requested_by`, `dispatched_at`, `received_at` |
| `iot_alert_rule` | oltp | `asset_id`, `metric_code`, `threshold_operator`, `threshold_value`, `unit_of_measure`, `cooldown_seconds`, `alert_severity`, `notify_users`, `notify_groups`, `is_multi_asset`, `target_asset_id`, `location_id`, `evaluate_on_bad_quality`, `correlation_id` |
| `iot_device_command` | oltp | `asset_id`, `command_type`, `protocol`, `payload`, `status`, `response_payload`, `expires_at` |
| `iot_device_profile` | oltp | `name`, `inbound_metric_rules`, `inbound_metadata_rules`, `outbound_command_templates` |
| `iot_harvester_config` | oltp | `asset_id`, `device_profile_id`, `target_url`, `port`, `polling_interval_seconds`, `schedule_type`, `cron_expression`, `auth_type`, `credentials`, `payload_format`, `http_method`, `http_headers`, `request_payload`, `url_query_params`, `mapping_directive` |
| `iot_subscription` | oltp | `asset_id`, `device_profile_id`, `aws_thing_name`, `topic_pattern`, `subscription_status`, `batch_window_seconds` |
| `labor_log` | oltp | `work_order_id`, `work_order_task_id`, `work_order_task_item_id`, `user_id`, `start_time`, `end_time`, `duration_minutes`, `hourly_rate`, `task_description` |
| `location` | oltp | `name`, `tag`, `floc_code`, `type`, `status`, `criticality`, `risk_probability_score`, `risk_impact_score`, `parent_location_id`, `description`, `omniclass_code`, `omniclass_name`, `coordinates_geojson`, `address_street`, `address_city`, `address_state_province`, `address_postal_code`, `address_country_iso2`, `area_value`, `area_unit`, `timezone`, `cost_center`, `primary_contact_user_id`, `custom_attributes`, `photo_ids` |
| `meter_reading` | olap | `asset_id`, `location_id`, `iot_subscription_id`, `device_profile_id`, `aws_thing_name`, `metric_code`, `reading_value`, `raw_value`, `unit_of_measure`, `terminology_system`, `data_quality`, `protocol`, `source_address`, `timestamp`, `ingested_at`, `metadata` |
| `note` | oltp | `content`, `author_id`, `timestamp`, `work_order_task_id` |
| `outbox_event` | oltp | `status`, `detail_type`, `payload`, `retry_count`, `retry_at`, `claimed_at`, `created_at` |
| `part` | oltp | `name`, `sku`, `barcode`, `description`, `category`, `default_unit_cost_cents`, `min_quantity`, `uom`, `currency` |
| `preventive_maintenance` | oltp | `asset_id`, `template_id`, `cron_expression`, `advance_notice_days`, `meter_based_trigger`, `advance_notice_meter_value`, `iana_timezone`, `prenotify_before_minutes`, `next_due_date`, `status` |
| `provider` | oltp | `name`, `contact_email`, `contact_phone`, `address`, `services_provided`, `status` |
| `reminder` | oltp | `title`, `message`, `target_user_id`, `target_group_id`, `reminder_datetime`, `prenotify_minutes_array`, `iana_timezone`, `status` |
| `request` | oltp | `title`, `description`, `requested_by_user`, `requested_by_email`, `form_template_id`, `form_data`, `status`, `priority`, `asset_id`, `location_id` |
| `role` | oltp | `name`, `description`, `grants`, `allowed_locations`, `allowed_assets`, `tenant_id` |
| `scheduled_job` | oltp | `parent_entity_ref`, `created_by`, `trigger_type`, `trigger_expression`, `iana_timezone`, `action_type`, `target_user_id`, `target_group_id`, `target_role_id`, `target_webhook_id`, `action_payload`, `idempotency_hash`, `status`, `last_run_at`, `run_count`, `last_error` |
| `sequence_registry` | oltp | `tenant_id`, `sequence_code`, `prefix`, `padding_length`, `current_value`, `parent_scope_tag` |
| `shift_pattern` | oltp | `name`, `user_id`, `user_group_id`, `grammar`, `cron_expression`, `iana_timezone`, `span_minutes`, `productive_factor`, `reactive_reserve_pct`, `effective_from`, `effective_to`, `status` |
| `task_template` | oltp | `name`, `description`, `estimated_duration_minutes`, `required_role_id` |
| `task_template_item` | oltp | `task_template_id`, `step_order`, `description` |
| `technician_shift` | oltp | `user_id`, `shift_date`, `start_time`, `end_time`, `status`, `shift_pattern_id`, `kind`, `absence_reason` |
| `tenant` | oltp | `name`, `tag`, `status`, `tier`, `require_mfa_for_new_users`, `mfa_policy`, `mfa_allowed_methods`, `session_max_idle_minutes`, `industry`, `timezone`, `currency`, `language`, `billing_admin_email`, `logo` |
| `tenant_plugin` | oltp | `tenant_id`, `plugin_id`, `status`, `config` |
| `user` | oltp | `username`, `password_hash`, `email`, `primary_phone`, `first_name`, `last_name`, `job_title`, `avatar`, `badge_id`, `status`, `user_type`, `role_ids`, `group_ids`, `tenant_id`, `company_id`, `primary_location_id`, `hourly_rate_cents`, `currency`, `skills`, `timezone`, `locale`, `failed_attempts`, `locked_until`, `mfa_enabled`, `mfa_secret`, `registration_method`, `registered_at`, `last_login_at` |
| `user_group` | oltp | `name`, `description`, `allowed_locations`, `allowed_assets`, `parent_user_group_id`, `time_restrictions` |
| `webhook_endpoint` | oltp | `name`, `target_url`, `http_method`, `authentication_type`, `auth_token`, `subscribed_rule_ids`, `max_retries`, `is_active` |
| `work_order` | oltp | `work_order_number`, `title`, `description`, `request_id`, `client_id`, `asset_id`, `location_id`, `assignees`, `assigned_group_ids`, `category`, `status`, `priority`, `due_date`, `total_cost_cents`, `currency`, `completion_percentage`, `checkin_latitude`, `checkin_longitude`, `checkin_at`, `checkout_latitude`, `checkout_longitude`, `checkout_at`, `is_geofence_verified`, `sla_response_due_date`, `sla_resolution_due_date`, `sla_response_breached`, `sla_resolution_breached`, `internal_audit_hash`, `scheduled_start`, `scheduled_end` |
| `work_order_task` | oltp | `work_order_id`, `task_template_id`, `asset_id`, `location_id`, `work_order_template_stop_id`, `description`, `assigned_user_id`, `assigned_group_id`, `estimated_duration_minutes`, `scheduled_start`, `scheduled_end`, `note_ids`, `status`, `evidence_file_ids`, `completed_by`, `completed_at` |
| `work_order_task_item` | oltp | `work_order_task_id`, `task_template_item_id`, `description`, `is_completed`, `note_ids`, `evidence_file_ids`, `completed_by`, `labor_log_ids`, `completed_at` |
| `work_order_template` | oltp | `title`, `description`, `assigned_role_id`, `assigned_group_id`, `estimated_duration_minutes`, `notification_template_code`, `status`, `subject_mode`, `scope_location_id`, `scope_include_descendants`, `scope_omniclass_code`, `scope_criticality` |
| `work_order_template_stop` | oltp | `work_order_template_id`, `stop_order`, `asset_id`, `location_id`, `task_template_id`, `estimated_effort_minutes`, `access_note` |
