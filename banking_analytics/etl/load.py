# etl/load.py
from datetime import datetime
import pandas as pd
import numpy as np


class DataLoader:
    """Загрузка данных в схему звезда"""

    def __init__(self, db_connection):
        self.db = db_connection

    def load_dimensions(self, customers_df, accounts_df, branches_df, date_dim_df):
        """Загрузка измерений"""
        print("\nЗагрузка dimension таблиц...")

        # dim_customer - с обогащенными данными
        customers_clean = customers_df[[
            'customer_id', 'first_name', 'last_name', 'full_name',
            'email', 'phone', 'age', 'city', 'country',
            'customer_segment', 'registration_date'
        ]].copy()
        customers_clean['effective_date'] = datetime.now().date()
        customers_clean['expiration_date'] = pd.to_datetime('2099-12-31').date()
        customers_clean['is_current'] = True

        self.db.load_dataframe(customers_clean, 'dim_customer', schema='dwh')

        # dim_account - без customer_id
        accounts_clean = accounts_df[[
            'account_id', 'account_number', 'account_type',
            'currency', 'opening_date', 'status'
        ]].copy()

        self.db.load_dataframe(accounts_clean, 'dim_account', schema='dwh')

        # dim_branch - ИСПРАВЛЕНО: без opening_date
        branches_clean = branches_df[[
            'branch_id', 'branch_name', 'city', 'region', 'address'
        ]].copy()

        self.db.load_dataframe(branches_clean, 'dim_branch', schema='dwh')

        # dim_date
        self.db.load_dataframe(date_dim_df, 'dim_date', schema='dwh')

        # dim_transaction_type
        transaction_types = pd.DataFrame([
            {'transaction_type': 'Deposit', 'transaction_category': 'Income',
             'description': 'Money deposit'},
            {'transaction_type': 'Withdrawal', 'transaction_category': 'Expense',
             'description': 'Cash withdrawal'},
            {'transaction_type': 'Transfer', 'transaction_category': 'Transfer',
             'description': 'Money transfer'},
            {'transaction_type': 'Payment', 'transaction_category': 'Expense',
             'description': 'Bill payment'},
            {'transaction_type': 'ATM', 'transaction_category': 'Expense',
             'description': 'ATM withdrawal'}
        ])
        self.db.load_dataframe(transaction_types, 'dim_transaction_type', schema='dwh')

        print("✓ Dimension таблицы загружены")

    def load_fact_table(self, transactions_df):
        """Загрузка фактовой таблицы"""
        print("\nЗагрузка fact таблицы...")

        # Получаем ключи из измерений
        customers_keys = self.db.read_query(
            "SELECT customer_key, customer_id FROM dwh.dim_customer WHERE is_current = TRUE"
        )
        accounts_keys = self.db.read_query(
            "SELECT account_key, account_id FROM dwh.dim_account"
        )
        transaction_type_keys = self.db.read_query(
            "SELECT transaction_type_key, transaction_type FROM dwh.dim_transaction_type"
        )

        # Обогащаем транзакции ключами
        transactions_enriched = transactions_df.merge(
            customers_keys, left_on='customer_id', right_on='customer_id', how='left'
        ).merge(
            accounts_keys, left_on='account_id', right_on='account_id', how='left'
        ).merge(
            transaction_type_keys, left_on='transaction_type', right_on='transaction_type', how='left'
        )

        # Создаем date_key
        transactions_enriched['date_key'] = pd.to_datetime(
            transactions_enriched['transaction_date']
        ).dt.strftime('%Y%m%d').astype(int)

        # Выбираем нужные колонки для fact таблицы
        fact_data = transactions_enriched[[
            'transaction_id', 'date_key', 'customer_key', 'account_key',
            'transaction_type_key', 'amount', 'currency', 'amount_rub',
            'exchange_rate', 'transaction_status', 'channel', 'merchant_name'
        ]].copy()

        # Переименовываем колонки
        fact_data = fact_data.rename(columns={
            'amount': 'amount_original',
            'currency': 'original_currency'
        })

        # Добавляем branch_key (случайно для демо)
        fact_data['branch_key'] = np.random.randint(1, 51, size=len(fact_data))

        # Удаляем строки с пропущенными ключами
        fact_data = fact_data.dropna(subset=['customer_key', 'account_key', 'transaction_type_key'])

        self.db.load_dataframe(fact_data, 'fact_transactions', schema='dwh')

        print("✓ Fact таблица загружена")
