CREATE TABLE NEYBYANDEXRU__STAGING.transactions
(
    operation_id uuid NOT NULL,
    account_number_from int NULL,
    account_number_to int NULL,
    currency_code int NULL,
    country varchar(100) NULL,
    status varchar(100) NULL,
    transaction_type varchar(100) NULL,
    amount int NULL,
    transaction_dt timestamp NULL
)
PARTITION BY (hash(date_trunc('day', transactions.transaction_dt)));

COMMENT ON COLUMN NEYBYANDEXRU__STAGING.transactions.operation_id IS 'Transaction ID';
COMMENT ON COLUMN NEYBYANDEXRU__STAGING.transactions.account_number_from IS 'Internal accounting number of the account the transaction is FROM';
COMMENT ON COLUMN NEYBYANDEXRU__STAGING.transactions.account_number_to IS 'Internal accounting number of the account the transaction is TO';
COMMENT ON COLUMN NEYBYANDEXRU__STAGING.transactions.currency_code IS 'Three-digit code of the currency of the transaction';
COMMENT ON COLUMN NEYBYANDEXRU__STAGING.transactions.country IS 'Country of the transaction source';
COMMENT ON COLUMN NEYBYANDEXRU__STAGING.transactions.status IS 'Transaction execution status: queued, in_progress, blocked, done, chargeback';
COMMENT ON COLUMN NEYBYANDEXRU__STAGING.transactions.transaction_type IS 'Type of transaction in internal accounting: authorisation, sbp_incoming, sbp_outgoing, transfer_incoming, transfer_outgoing, c2b_partner_incoming, c2b_partner_outgoing';
COMMENT ON COLUMN NEYBYANDEXRU__STAGING.transactions.amount IS 'Integer amount of the transaction in the smallest currency unit (e.g., penny, cent, kuruş)';
COMMENT ON COLUMN NEYBYANDEXRU__STAGING.transactions.transaction_dt IS 'Date and time of transaction execution in milliseconds';


CREATE PROJECTION NEYBYANDEXRU__STAGING.transactions /*+createtype(P)*/ 
(
 operation_id,
 account_number_from,
 account_number_to,
 currency_code,
 country,
 status,
 transaction_type,
 amount,
 transaction_dt
)
AS
 SELECT transactions.operation_id,
        transactions.account_number_from,
        transactions.account_number_to,
        transactions.currency_code,
        transactions.country,
        transactions.status,
        transactions.transaction_type,
        transactions.amount,
        transactions.transaction_dt
 FROM NEYBYANDEXRU__STAGING.transactions
 ORDER BY transactions.transaction_dt,
          transactions.operation_id
SEGMENTED BY hash(transactions.transaction_dt, transactions.operation_id) ALL NODES KSAFE 1;
