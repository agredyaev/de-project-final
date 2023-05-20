CREATE TABLE NEYBYANDEXRU__DWH.global_metrics
(
    date_update timestamp,
    currency_from int,
    amount_total float,
    cnt_transactions int,
    avg_transactions_per_account float,
    cnt_accounts_make_transactions int
)
PARTITION BY (hash(date_trunc('day', global_metrics.date_update)));

COMMENT ON COLUMN NEYBYANDEXRU__DWH.global_metrics.date_update IS 'Calculation date';
COMMENT ON COLUMN NEYBYANDEXRU__DWH.global_metrics.currency_from IS 'Transaction currency code';
COMMENT ON COLUMN NEYBYANDEXRU__DWH.global_metrics.amount_total IS 'Total transaction amount in dollars for the currency';
COMMENT ON COLUMN NEYBYANDEXRU__DWH.global_metrics.cnt_transactions IS 'Total volume of transactions for the currency';
COMMENT ON COLUMN NEYBYANDEXRU__DWH.global_metrics.avg_transactions_per_account IS 'Average transaction volume per account';
COMMENT ON COLUMN NEYBYANDEXRU__DWH.global_metrics.cnt_accounts_make_transactions IS 'Number of unique accounts with transactions for the currency';