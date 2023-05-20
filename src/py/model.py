from pydantic import BaseModel, validator
from uuid import UUID
from datetime import datetime

timestamp_template = "%Y-%m-%d %H:%M:%S.%f"

class Currency(BaseModel):
    date_update: datetime
    currency_code: int
    currency_code_with: int
    currency_code_div: float

    @validator('date_update')
    def format_timestamp(cls, value):
        formatted_timestamp = value.strftime(timestamp_template)[:-3]
        return formatted_timestamp


class Transaction(BaseModel):
    operation_id: UUID
    account_number_from: int
    account_number_to: int
    currency_code: int
    country: str
    status: str
    transaction_type: str
    amount: int
    transaction_dt: datetime

    @validator('transaction_dt')
    def format_timestamp(cls, value):
        formatted_timestamp = value.strftime(timestamp_template)[:-3]
        return formatted_timestamp


class GlobalMetrics(BaseModel):
    date_update: datetime
    currency_from: int
    amount_total: float
    cnt_transactions: int
    avg_transactions_per_account: float
    cnt_accounts_make_transactions: int

    @validator('date_update')
    def format_timestamp(cls, value):
        formatted_timestamp = value.strftime(timestamp_template)[:-3]
        return formatted_timestamp
