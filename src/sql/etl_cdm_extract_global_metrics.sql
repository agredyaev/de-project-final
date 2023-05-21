with
_usd_currency_rate as (
	select 
		distinct date_trunc('day', date_update) date_update,
		currency_code,
		currency_code_div 
	from NEYBYANDEXRU__STAGING.currencies
	where currency_code_with=420 -- usa currency code
),
_transactions as (
	select
		t.*,
		isnull(ucr.currency_code_div, 1) usd_currency_rate 
	from NEYBYANDEXRU__STAGING.transactions t
	left join _usd_currency_rate ucr on (
		ucr.currency_code=t.currency_code
		and ucr.date_update=date_trunc('day', t.transaction_dt)
	)
	where account_number_from > 0
),
_transactions_per_account as (
	select
		date_trunc('day', transaction_dt)::date date_update,
		currency_code,
		account_number_from,
		count(*) transactions_per_account,
		sum(amount * usd_currency_rate) amount_total
	from _transactions
	group by date_trunc('day', transaction_dt),
		date_update,
		currency_code,
		account_number_from
)
select 
	date_update,
	currency_code currency_from,
	sum(amount_total) amount_total,
	sum(transactions_per_account) cnt_transactions,
	avg(amount_total) avg_transactions_per_account,
	count(distinct account_number_from) cnt_accounts_make_transactions
from _transactions_per_account
where extract(day from (date_update - interval '1 DAY')-'{{ variable }}')=0
group by date_update, currency_code;