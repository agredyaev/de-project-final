INSERT INTO
    NEYBYANDEXRU__STAGING.currencies (
        date_update,
        currency_code,
        currency_code_with,
        currency_code_div
    )
    SELECT 
        :date_update, 
        :currency_code, 
        :currency_code_with, 
        :currency_code_div
    WHERE NOT EXISTS (
        SELECT 1 FROM NEYBYANDEXRU__STAGING.currencies 
        WHERE date_update = :date_update
        AND currency_code = :currency_code
        AND currency_code_with = :currency_code_with
    );