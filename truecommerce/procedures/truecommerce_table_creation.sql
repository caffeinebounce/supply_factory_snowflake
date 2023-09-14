use database prod_sf_tables; 
create schema truecommerce;
create or replace table truecommerce.transaction_register ( 
    transaction_id VARCHAR(64),
    index INT,
    trading_partner VARCHAR(255),
    trading_partner_qb_id VARCHAR(50),
    transaction_type VARCHAR(255),
    transaction_type_id VARCHAR(50),
    doc_number VARCHAR(50), 
    alt_doc_number VARCHAR(50),
    amount FLOAT,
    creation_date DATE,
    folder VARCHAR(255),
    action_date DATE,
    action_name VARCHAR(255),
    action_status VARCHAR(255),
    period_first_day DATE,
    period_last_day DATE,
    latest boolean,
    _row_id VARCHAR(64),
    _modified VARCHAR(255),
    _file VARCHAR(255),
    _fivetran_synced TIMESTAMP_TZ(9)
);

create or replace table truecommerce.qb_mapping (
    qb_id VARCHAR(50),
    qb_name VARCHAR(255),
    qb_fq_name VARCHAR(255),
    tc_name VARCHAR(255)
);

create or replace table truecommerce.integration ( 
    transaction_id VARCHAR(64),
    index INT,
    trading_partner VARCHAR(255),
    trading_partner_qb_id VARCHAR(50),
    transaction_type VARCHAR(255),
    alt_doc_number VARCHAR(50),
    tc_account_id VARCHAR(64),
    amount FLOAT,
    document_date TIMESTAMP_TZ(9),
    log_time TIMESTAMP_TZ(9),
    description VARCHAR(1024),
    period_first_day DATE,
    period_last_day DATE,
    latest boolean,
    _row_id VARCHAR(64),
    _modified VARCHAR(255),
    _file VARCHAR(255),
    _fivetran_synced TIMESTAMP_TZ(9)
);