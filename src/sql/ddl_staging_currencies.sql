CREATE TABLE NEYBYANDEXRU__STAGING.сurrencies
(
    date_update timestamp,
    currency_code int,
    currency_code_with int,
    currency_code_div float
)
PARTITION BY (hash(date_trunc('day', сurrencies.date_update)));

COMMENT ON COLUMN NEYBYANDEXRU__STAGING.currencies.date_update IS 'Date of currency rate update';
COMMENT ON COLUMN NEYBYANDEXRU__STAGING.currencies.currency_code IS 'Three-digit code of the transaction currency';
COMMENT ON COLUMN NEYBYANDEXRU__STAGING.currencies.currency_code_with IS 'Ratio of another currency to the currency with a three-digit code';
COMMENT ON COLUMN NEYBYANDEXRU__STAGING.currencies.currency_code_div IS 'Value of the ratio of one unit of another currency to one unit of the transaction currency';

CREATE PROJECTION NEYBYANDEXRU__STAGING.сurrencies /*+createtype(P)*/ 
(
 date_update,
 currency_code,
 currency_code_with,
 currency_code_div
)
AS
 SELECT сurrencies.date_update,
        сurrencies.currency_code,
        сurrencies.currency_code_with,
        сurrencies.currency_code_div
 FROM NEYBYANDEXRU__STAGING.сurrencies
 ORDER BY сurrencies.date_update
SEGMENTED BY hash(сurrencies.date_update, сurrencies.currency_code, сurrencies.currency_code_with) ALL NODES KSAFE 1;
