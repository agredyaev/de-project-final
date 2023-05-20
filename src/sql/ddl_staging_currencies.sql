CREATE TABLE NEYBYANDEXRU__STAGING.currencies
(
    date_update timestamp null,
    currency_code int null,
    currency_code_with int null,
    currency_code_div float null
)
PARTITION BY (hash(date_trunc('day', currencies.date_update)));

COMMENT ON COLUMN NEYBYANDEXRU__STAGING.currencies.date_update IS 'Date of currency rate update';
COMMENT ON COLUMN NEYBYANDEXRU__STAGING.currencies.currency_code IS 'Three-digit code of the transaction currency';
COMMENT ON COLUMN NEYBYANDEXRU__STAGING.currencies.currency_code_with IS 'Ratio of another currency to the currency with a three-digit code';
COMMENT ON COLUMN NEYBYANDEXRU__STAGING.currencies.currency_code_div IS 'Value of the ratio of one unit of another currency to one unit of the transaction currency';

CREATE PROJECTION NEYBYANDEXRU__STAGING.currencies /*+createtype(P)*/ 
(
 date_update,
 currency_code,
 currency_code_with,
 currency_code_div
)
AS
 SELECT currencies.date_update,
        currencies.currency_code,
        currencies.currency_code_with,
        currencies.currency_code_div
 FROM NEYBYANDEXRU__STAGING.currencies
 ORDER BY currencies.date_update
SEGMENTED BY hash(currencies.date_update, currencies.currency_code, currencies.currency_code_with) ALL NODES KSAFE 1;
