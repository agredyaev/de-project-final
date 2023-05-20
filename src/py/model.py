from pydantic import BaseModel
from uuid import UUID
from datetime import datetime

class Currency(BaseModel):
    date_update: datetime
    currency_code: int
    currency_code_with: int
    currency_code_div: float

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

class GlobalMetrics(BaseModel):
    date_update: datetime
    currency_from: int
    amount_total: float
    cnt_transactions: int
    avg_transactions_per_account: float
    cnt_accounts_make_transactions: int