select
    date_update,
    currency_code,
    currency_code_with,
    currency_with_div
from
    public.currencies
where extract(day from date_update-'{{ variable }}')=0;