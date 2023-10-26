-------------------------------------------------------------
--STG layer entityes
-------------------------------------------------------------
DROP TABLE IF EXISTS DEAIAN.YUNV_STG_terminals;
CREATE TABLE IF NOT EXISTS DEAIAN.YUNV_STG_terminals(
    terminal_id VARCHAR,
    terminal_type VARCHAR,
    terminal_city VARCHAR,
    terminal_address VARCHAR,
    processed_dt DATE
);

DROP TABLE IF EXISTS DEAIAN.YUNV_STG_clients;
CREATE TABLE IF NOT EXISTS DEAIAN.YUNV_STG_clients(
    client_id VARCHAR(10),
    last_name VARCHAR(20),
    first_name VARCHAR(20),
    patronymic VARCHAR(20),
    date_of_birth DATE,
    passport_num VARCHAR(15),
    passport_valid_to DATE,
    phone BPCHAR(16),
    create_dt TIMESTAMP(1),
    update_dt TIMESTAMP(1)
);

DROP TABLE IF EXISTS DEAIAN.YUNV_STG_accounts;
CREATE TABLE IF NOT EXISTS DEAIAN.YUNV_STG_accounts(
    account BPCHAR(20),
    valid_to DATE,
    client VARCHAR(10),
    create_dt TIMESTAMP(1),
    update_dt TIMESTAMP(1)
);

DROP TABLE IF EXISTS DEAIAN.YUNV_STG_cards;
CREATE TABLE IF NOT EXISTS DEAIAN.YUNV_STG_cards(
    card_num BPCHAR(20),
    account BPCHAR(20),
    create_dt TIMESTAMP(1),
    update_dt TIMESTAMP(1)
);

DROP TABLE IF EXISTS DEAIAN.YUNV_STG_transactions;
CREATE TABLE IF NOT EXISTS DEAIAN.YUNV_STG_transactions(
    trans_id VARCHAR,
    trans_date TIMESTAMP(1),
    amt DECIMAL,
    card_num BPCHAR(20),
    oper_type VARCHAR,
    oper_result VARCHAR,
    terminal VARCHAR
);

DROP TABLE IF EXISTS DEAIAN.YUNV_STG_passport_blacklist;
CREATE TABLE IF NOT EXISTS DEAIAN.YUNV_STG_passport_blacklist(
    entry_dt DATE,
    passport_num VARCHAR(15)
);

----------------------------------------------------------------------------
--DWH layer entityes
----------------------------------------------------------------------------
DROP TABLE IF EXISTS DEAIAN.YUNV_DWH_DIM_terminals_HIST;
CREATE TABLE IF NOT EXISTS DEAIAN.YUNV_DWH_DIM_terminals_HIST(
    terminal_id VARCHAR,
    terminal_type VARCHAR,
    terminal_city VARCHAR,
    terminal_address VARCHAR,
    effective_from DATE,
    effective_to DATE,
    deleted_flg VARCHAR(1),
    file_date DATE
);

DROP TABLE IF EXISTS DEAIAN.YUNV_DWH_DIM_clients_HIST;
CREATE TABLE IF NOT EXISTS DEAIAN.YUNV_DWH_DIM_clients_HIST(
    client_id VARCHAR(10),
    last_name VARCHAR(20),
    first_name VARCHAR(20),
    patronymic VARCHAR(20),
    date_of_birth DATE,
    passport_num VARCHAR(15),
    passport_valid_to DATE,
    phone BPCHAR(16),
    effective_from TIMESTAMP(1),
    effective_to TIMESTAMP(1),
    processed_dt TIMESTAMP(1),
    deleted_flg VARCHAR(1)
);

DROP TABLE IF EXISTS DEAIAN.YUNV_DWH_DIM_accounts_HIST;
CREATE TABLE IF NOT EXISTS DEAIAN.YUNV_DWH_DIM_accounts_HIST(
    account_num BPCHAR(20),
    valid_to DATE,
    client VARCHAR(10),
    effective_from TIMESTAMP(1),
    effective_to TIMESTAMP(1),
    processed_dt TIMESTAMP(1),
    deleted_flg VARCHAR(1)
);

DROP TABLE IF EXISTS DEAIAN.YUNV_DWH_DIM_cards_HIST;
CREATE TABLE IF NOT EXISTS DEAIAN.YUNV_DWH_DIM_cards_HIST(
    card_num BPCHAR(20),
    account_num BPCHAR(20),
    effective_from TIMESTAMP(1),
    effective_to TIMESTAMP(1),
    processed_dt TIMESTAMP(1),
    deleted_flg VARCHAR(1)
);

DROP TABLE IF EXISTS DEAIAN.YUNV_DWH_FACT_transactions;
CREATE TABLE IF NOT EXISTS DEAIAN.YUNV_DWH_FACT_transactions(
    trans_id VARCHAR,
    trans_date TIMESTAMP(1),
    amt DECIMAL,
    card_num BPCHAR(20),
    oper_type VARCHAR,
    oper_result VARCHAR,
    terminal VARCHAR,
    file_date DATE
);

DROP TABLE IF EXISTS DEAIAN.YUNV_DWH_FACT_passport_blacklist;
CREATE TABLE IF NOT EXISTS DEAIAN.YUNV_DWH_FACT_passport_blacklist(
    entry_dt DATE,
    passport_num VARCHAR(15),
    processed_dt TIMESTAMP(1),
    file_date DATE
);

---------------------------------------------------------------------
--REP layer entityes внесены изменения
---------------------------------------------------------------------
DROP TABLE IF EXISTS DEAIAN.YUNV_REP_FRAUD;
DROP TABLE IF EXISTS DEAIAN.YUNV_REP_event_type;
CREATE TABLE IF NOT EXISTS DEAIAN.YUNV_REP_event_type(
    event_type INT PRIMARY KEY,
    description TEXT,
    effective_from TIMESTAMP(1) DEFAULT NOW()::TIMESTAMP(1),
    effective_to TIMESTAMP(1) DEFAULT to_date ('9999-12-31', 'YYYY-MM-DD')::TIMESTAMP(1),
    deleted_flg VARCHAR
);

INSERT INTO DEAIAN.YUNV_REP_event_type(event_type, description, deleted_flg)
    VALUES(1, 'Совершение операции при просроченном или заблокированном паспорте', 'N');
INSERT INTO DEAIAN.YUNV_REP_event_type(event_type, description, deleted_flg)
    VALUES(2, 'Совершение операции при недействующем договоре', 'N');
INSERT INTO DEAIAN.YUNV_REP_event_type(event_type, description, deleted_flg)
    VALUES(3, 'Совершение операций в разных городах в течене одного часа', 'N');
INSERT INTO DEAIAN.YUNV_REP_event_type(event_type, description, deleted_flg)
    VALUES(4, 'Попытка подбора суммы', 'N');


DROP TABLE IF EXISTS DEAIAN.YUNV_REP_FRAUD;
CREATE TABLE IF NOT EXISTS DEAIAN.YUNV_REP_FRAUD(
    event_dt TIMESTAMP(1),
    passport VARCHAR(15),
    fio VARCHAR,
    phone BPCHAR(16),
    event_type INT REFERENCES DEAIAN.YUNV_REP_event_type(event_type),
    report_dt DATE
);

---------------------------------------------------------------------
--META layer entityes
---------------------------------------------------------------------
DROP TABLE IF EXISTS DEAIAN.YUNV_META_DATE_POINTS;
CREATE TABLE IF NOT EXISTS DEAIAN.YUNV_META_DATE_POINTS(
    date_point DATE NOT NULL,
    transactions_flg INT DEFAULT 0,
    passport_blacklist_flg INT DEFAULT 0,
    terminals_flg INT DEFAULT 0,
    event_type_1_rep INT DEFAULT 0,
    event_type_2_rep INT DEFAULT 0,
    event_type_3_rep INT DEFAULT 0,
    event_type_4_rep INT DEFAULT 0,
    event_type_all_rep INT DEFAULT 0,
    effective_from TIMESTAMP(1) NOT NULL DEFAULT NOW()::TIMESTAMP(1),
    effective_to TIMESTAMP(1) NOT NULL DEFAULT NOW()::TIMESTAMP(1)
);

DROP TABLE IF exists deaian.yunv_meta;
CREATE TABLE IF NOT EXISTS deaian.yunv_meta(
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt TIMESTAMP(1)
);
--insert into deaian.yunv_meta(schema_name, table_name, max_update_dt)
    --values('bank', 'clients', to_date ('1900-01-01','YYYY-MM-DD')::timstamp(0));
--insert into deaian.yunv_meta(schema_name, table_name, max_update_dt)
    --values('bank', 'accounts', to_date ('1900-01-01','YYYY-MM-DD')::timstamp(0));
--insert into deaian.yunv_meta(schema_name, table_name, max_update_dt)
    --values('bank', 'cards', to_date ('1900-01-01','YYYY-MM-DD')::timstamp(0));

DROP TABLE IF exists deaian.yunv_stg_del;
CREATE TABLE IF NOT EXISTS deaian.yunv_stg_del(
    id varchar(20)
);