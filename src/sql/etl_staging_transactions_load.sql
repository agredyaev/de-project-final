INSERT INTO 
    NEYBYANDEXRU__STAGING.transactions (
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
SELECT 
    :operation_id, 
    :account_number_from, 
    :account_number_to, 
    :currency_code, 
    :country, 
    :status, 
    :transaction_type, 
    :amount, 
    :transaction_dt
WHERE NOT EXISTS (
    SELECT 1 FROM NEYBYANDEXRU__STAGING.transactions 
    WHERE operation_id = :operation_id
    AND transaction_dt = :transaction_dt

);