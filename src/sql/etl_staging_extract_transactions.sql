SELECT
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount,
    transaction_dt
FROM
    public.transactions
where extract(day from transaction_dt-'{{ variable }}')=0;