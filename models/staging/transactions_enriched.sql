{{ config(
    materialized='incremental',
    incremental_strategy= 'append',
    on_schema_change = 'sync_all_columns'
)}}

select 
    hash
    , block_number
    , date
    , from_address
    , to_address
    , value
    , receipt_contract_address
    , input
    , 1 as new_campo
from {{ source('eth','transactions')}}

{% if is_incremental() %}

where date >= (select max(date) from {{ this }} )

{% endif %}
