drop table if exists prod_b.seq_0_to_10;
CREATE TABLE prod_b.seq_0_to_10 AS (
  SELECT 0 AS i UNION ALL
  SELECT 1 UNION ALL
  SELECT 2 UNION ALL
  SELECT 3 UNION ALL
  SELECT 4 UNION ALL
  SELECT 5 UNION ALL
  SELECT 6 UNION ALL
  SELECT 7 UNION ALL
  SELECT 8 UNION ALL
  SELECT 9 UNION ALL
  SELECT 10
);
drop table if exists prod_b.tmp_pi_aggregated;
create table prod_b.tmp_pi_aggregated as
(
WITH exploded_array AS (
  SELECT
         payment_authentication_id 
       , payment_session_id
       , prior_authentication_id
       , payment_instrument_id
       , payment_authentication_data_reference_id
       , bin
       , payment_sub_method
       , bin_issuing_country
       , authentication_amount
       , authentication_currency_code
       , authentication_acquirer_mid
       , authentication_acquirer_country
       , enforce_authentication
       , bypass_authentication_failures
       , authentication_category
       , risk_transaction_id
       , risk_transaction_data_sent
       , risk_transaction_data_status_code
       , three_ds_terminal_state
       , three_ds_requester_challenge_indicator
       , three_ds_requester_challenge_indicator_description
       , acs_challenge_mandated
       , authentication_type
       , authentication_type_description
       , transaction_status
       , transaction_status_description
       , challenge_cancel_reason
       , challenge_cancel_description
       , three_ds_provider
       , three_ds_provider_mid
       , init_reference_id
       , acs_transaction_id
       , directory_server_transaction_id
       , three_ds_server_transaction_id
       , three_ds_version
       , enrollment_response
       , device_channel
       , requester_initiated_type
       , requester_initiated_description
       , transaction_category
       , allow_further_payment_processing
       , reason_code
       , reason_description
       , authentication_created_dt
       , challenge_requested_dt
       , init_requested_dt
       , authentication_completed_dt
       , payment_intents
       , provider_operations
       , etl_file_name
       , etl_version
       , etl_inserted_dt
       , JSON_EXTRACT_ARRAY_ELEMENT_TEXT(payment_intents,seq.i) AS payment_intents_aggregated
  FROM pdm.prod_b.fact_tds_authentication, prod_b.seq_0_to_10 AS seq
  WHERE payment_intents is not null
  AND seq.i < JSON_ARRAY_LENGTH(payment_intents)
)
  SELECT
  
         payment_authentication_id 
       , payment_session_id
       , prior_authentication_id
       , payment_instrument_id
       , payment_authentication_data_reference_id
       , bin
       , payment_sub_method
       , bin_issuing_country
       , authentication_amount
       , authentication_currency_code
       , authentication_acquirer_mid
       , authentication_acquirer_country
       , enforce_authentication
       , bypass_authentication_failures
       , authentication_category
       , risk_transaction_id
       , risk_transaction_data_sent
       , risk_transaction_data_status_code
       , three_ds_terminal_state
       , three_ds_requester_challenge_indicator
       , three_ds_requester_challenge_indicator_description
       , acs_challenge_mandated
       , authentication_type
       , authentication_type_description
       , transaction_status
       , transaction_status_description
       , challenge_cancel_reason
       , challenge_cancel_description
       , three_ds_provider
       , three_ds_provider_mid
       , init_reference_id
       , acs_transaction_id
       , directory_server_transaction_id
       , three_ds_server_transaction_id
       , three_ds_version
       , enrollment_response
       , device_channel
       , requester_initiated_type
       , requester_initiated_description
       , transaction_category
       , allow_further_payment_processing
       , reason_code
       , reason_description
       , authentication_created_dt
       , challenge_requested_dt
       , init_requested_dt
       , authentication_completed_dt
       , payment_intents
       , provider_operations
       , etl_file_name
       , etl_version
       , etl_inserted_dt
       , json_extract_path_text(payment_intents_aggregated,'business_model') as business_model
  FROM exploded_array
)
union
(
  SELECT
         payment_authentication_id 
       , payment_session_id
       , prior_authentication_id
       , payment_instrument_id
       , payment_authentication_data_reference_id
       , bin
       , payment_sub_method
       , bin_issuing_country
       , authentication_amount
       , authentication_currency_code
       , authentication_acquirer_mid
       , authentication_acquirer_country
       , enforce_authentication
       , bypass_authentication_failures
       , authentication_category
       , risk_transaction_id
       , risk_transaction_data_sent
       , risk_transaction_data_status_code
       , three_ds_terminal_state
       , three_ds_requester_challenge_indicator
       , three_ds_requester_challenge_indicator_description
       , acs_challenge_mandated
       , authentication_type
       , authentication_type_description
       , transaction_status
       , transaction_status_description
       , challenge_cancel_reason
       , challenge_cancel_description
       , three_ds_provider
       , three_ds_provider_mid
       , init_reference_id
       , acs_transaction_id
       , directory_server_transaction_id
       , three_ds_server_transaction_id
       , three_ds_version
       , enrollment_response
       , device_channel
       , requester_initiated_type
       , requester_initiated_description
       , transaction_category
       , allow_further_payment_processing
       , reason_code
       , reason_description
       , authentication_created_dt
       , challenge_requested_dt
       , init_requested_dt
       , authentication_completed_dt
       , payment_intents
       , provider_operations
       , etl_file_name
       , etl_version
       , etl_inserted_dt
       , NULL
  FROM pdm.prod_b.fact_tds_authentication, prod_b.seq_0_to_10 AS seq
  WHERE payment_intents is null)
;
DROP VIEW IF EXISTS prod_b.ods_fact_tds_authentication_busmodel_list;
DROP VIEW IF EXISTS prod_b.ods_fact_tds_authentication_busmodel_list;
drop table if exists prod_b.tmp_business_model_list;
create table prod_b.tmp_business_model_list as
SELECT distinct payment_authentication_id,
     payment_session_id,
     payment_instrument_id,
	 payment_authentication_data_reference_id,
	 bin,
	 payment_sub_method,
	 bin_issuing_country,
	 authentication_amount,
	 authentication_currency_code,
	 authentication_acquirer_mid,
	 authentication_acquirer_country,
     enforce_authentication,
     bypass_authentication_failures,
     authentication_category,
     risk_transaction_id,
     risk_transaction_data_sent,
     risk_transaction_data_status_code,
     three_ds_terminal_state,
     three_ds_requester_challenge_indicator,
     three_ds_requester_challenge_indicator_description,
     acs_challenge_mandated,
     authentication_type,
     authentication_type_description,
     transaction_status,
     transaction_status_description,
     challenge_cancel_reason,
     challenge_cancel_description,
     three_ds_provider,
     three_ds_provider_mid,
     init_reference_id,
     acs_transaction_id,
     directory_server_transaction_id,
     three_ds_server_transaction_id,
     three_ds_version,
     enrollment_response,
     device_channel,
     requester_initiated_type,
     requester_initiated_description,
     transaction_category,
     allow_further_payment_processing,
     reason_code,
     reason_description,
     authentication_created_dt,
     challenge_requested_dt,
     init_requested_dt,
     authentication_completed_dt,
     payment_intents,
     provider_operations,
     etl_file_name,
     etl_version,
     etl_inserted_dt,
     LISTAGG(business_model,', ')
    --
WITHIN GROUP (ORDER BY business_model)
OVER (PARTITION BY payment_authentication_id) AS business_model_list
FROM prod_b.tmp_pi_aggregated;

CREATE VIEW prod_b.ods_fact_tds_authentication_busmodel_list
AS
select * from prod_b.tmp_business_model_list;
Grant all on prod_b.ods_fact_tds_authentication_busmodel_list  to group pdm_admin;
Grant all on prod_b.ods_fact_tds_authentication_busmodel_list  to group pdm_etl;
Grant select on prod_b.ods_fact_tds_authentication_busmodel_list  to group pdm_power_user;
Grant select on prod_b.ods_fact_tds_authentication_busmodel_list  to group pdm_user;
Grant select on prod_b.ods_fact_tds_authentication_busmodel_list  to group pdm_report;

