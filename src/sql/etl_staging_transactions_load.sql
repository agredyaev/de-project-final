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
VALUES
    (
        {operation_id},
        {account_number_from},
        {account_number_to},
        {currency_code},
        {country},
        {status},
        {transaction_type},
        {amount},
        {transaction_dt}
    )