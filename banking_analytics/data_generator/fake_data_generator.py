from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta

fake = Faker('ru_RU')


class BankingDataGenerator:
    """Генератор синтетических банковских данных"""

    def __init__(self, num_customers=1000, num_transactions=10000):
        self.num_customers = num_customers
        self.num_transactions = num_transactions

    def generate_customers(self):
        """Генерация данных о клиентах"""
        customers = []
        for i in range(1, self.num_customers + 1):
            customers.append({
                'customer_id': i,
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'email': fake.email(),
                'phone': fake.phone_number(),
                'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=80),
                'city': fake.city(),
                'country': 'Russia',
                'registration_date': fake.date_between(start_date='-5y', end_date='today'),
                'customer_segment': random.choice(['Retail', 'Premium', 'Corporate'])
            })
        return pd.DataFrame(customers)

    def generate_accounts(self, num_customers):
        """Генерация банковских счетов"""
        accounts = []
        account_id = 1
        for customer_id in range(1, num_customers + 1):
            # У каждого клиента от 1 до 3 счетов
            num_accounts = random.randint(1, 3)
            for _ in range(num_accounts):
                accounts.append({
                    'account_id': account_id,
                    'customer_id': customer_id,
                    'account_number': fake.bban(),
                    'account_type': random.choice(['Checking', 'Savings', 'Credit', 'Investment']),
                    'currency': random.choice(['RUB', 'USD', 'EUR']),
                    'balance': round(random.uniform(1000, 1000000), 2),
                    'opening_date': fake.date_between(start_date='-3y', end_date='today'),
                    'status': random.choice(['Active', 'Active', 'Active', 'Frozen', 'Closed'])
                })
                account_id += 1
        return pd.DataFrame(accounts)

    def generate_transactions(self, accounts_df):
        """Генерация транзакций"""
        transactions = []
        account_ids = accounts_df['account_id'].tolist()

        for i in range(1, self.num_transactions + 1):
            transaction_date = fake.date_time_between(start_date='-1y', end_date='now')
            transactions.append({
                'transaction_id': i,
                'account_id': random.choice(account_ids),
                'transaction_date': transaction_date,
                'transaction_type': random.choice(['Deposit', 'Withdrawal', 'Transfer', 'Payment', 'ATM']),
                'amount': round(random.uniform(100, 50000), 2),
                'currency': random.choice(['RUB', 'USD', 'EUR']),
                'merchant_name': fake.company() if random.random() > 0.3 else None,
                'transaction_status': random.choice(['Completed', 'Completed', 'Pending', 'Failed']),
                'channel': random.choice(['Online', 'Mobile', 'ATM', 'Branch'])
            })
        return pd.DataFrame(transactions)

    def generate_branches(self):
        """Генерация данных о банковских отделениях"""
        branches = []
        for i in range(1, 51):
            branches.append({
                'branch_id': i,
                'branch_name': f'Branch {i}',
                'city': fake.city(),
                'address': fake.address(),
                'region': random.choice(['Central', 'North', 'South', 'East', 'West']),
                'opening_date': fake.date_between(start_date='-10y', end_date='-1y')
            })
        return pd.DataFrame(branches)
