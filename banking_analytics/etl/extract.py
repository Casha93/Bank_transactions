# etl/extract.py
"""
Модуль извлечения данных из источников (Extract фаза ETL)
"""
import pandas as pd
from database.db_connection import DatabaseConnection


class DataExtractor:
    """Класс для извлечения данных из различных источников"""

    def __init__(self, db_connection: DatabaseConnection):
        """
        Инициализация экстрактора данных

        Args:
            db_connection: экземпляр подключения к базе данных
        """
        self.db = db_connection

    def extract_customers_from_staging(self):
        """
        Извлечение данных клиентов из staging-слоя

        Returns:
            DataFrame с данными клиентов
        """
        query = """
        SELECT 
            customer_id,
            first_name,
            last_name,
            email,
            phone,
            date_of_birth,
            city,
            country,
            registration_date,
            customer_segment
        FROM staging.customers
        WHERE customer_id IS NOT NULL
        """

        print("Извлечение данных клиентов из staging...")
        df = self.db.read_query(query)
        print(f"Извлечено {len(df)} записей клиентов")
        return df

    def extract_accounts_from_staging(self):
        """
        Извлечение данных счетов из staging-слоя

        Returns:
            DataFrame с данными счетов
        """
        query = """
        SELECT 
            account_id,
            customer_id,
            account_number,
            account_type,
            currency,
            balance,
            opening_date,
            status
        FROM staging.accounts
        WHERE account_id IS NOT NULL
        """

        print("Извлечение данных счетов из staging...")
        df = self.db.read_query(query)
        print(f"Извлечено {len(df)} записей счетов")
        return df

    def extract_transactions_from_staging(self):
        """
        Извлечение транзакций из staging-слоя

        Returns:
            DataFrame с транзакциями
        """
        query = """
        SELECT 
            t.transaction_id,
            t.account_id,
            t.transaction_date,
            t.transaction_type,
            t.amount,
            t.currency,
            t.merchant_name,
            t.transaction_status,
            t.channel,
            a.customer_id
        FROM staging.transactions t
        LEFT JOIN staging.accounts a ON t.account_id = a.account_id
        WHERE t.transaction_id IS NOT NULL
            AND t.transaction_status = 'Completed'
        """

        print("Извлечение транзакций из staging...")
        df = self.db.read_query(query)
        print(f"Извлечено {len(df)} транзакций")
        return df

    def extract_branches_from_staging(self):
        """
        Извлечение данных отделений из staging-слоя

        Returns:
            DataFrame с данными отделений
        """
        query = """
        SELECT 
            branch_id,
            branch_name,
            city,
            address,
            region,
            opening_date
        FROM staging.branches
        WHERE branch_id IS NOT NULL
        """

        print("Извлечение данных отделений из staging...")
        df = self.db.read_query(query)
        print(f"Извлечено {len(df)} записей отделений")
        return df

    def extract_exchange_rates_from_staging(self):
        """
        Извлечение курсов валют из staging-слоя

        Returns:
            DataFrame с курсами валют
        """
        query = """
        SELECT 
            date,
            usd_to_rub,
            eur_to_rub,
            usd_to_eur
        FROM staging.exchange_rates
        ORDER BY date DESC
        LIMIT 1
        """

        print("Извлечение курсов валют из staging...")
        df = self.db.read_query(query)
        print(f"Извлечено курсов на дату: {df['date'].iloc[0] if not df.empty else 'N/A'}")
        return df

    def extract_all_staging_data(self):
        """
        Извлечение всех данных из staging одним вызовом

        Returns:
            dict: словарь с DataFrames для каждой таблицы
        """
        print("\n=== Начало извлечения данных из staging ===\n")

        data = {
            'customers': self.extract_customers_from_staging(),
            'accounts': self.extract_accounts_from_staging(),
            'transactions': self.extract_transactions_from_staging(),
            'branches': self.extract_branches_from_staging(),
            'exchange_rates': self.extract_exchange_rates_from_staging()
        }

        print("\n=== Извлечение данных завершено ===\n")
        return data

    def extract_from_csv(self, file_path):
        """
        Дополнительный метод: извлечение данных из CSV файла

        Args:
            file_path: путь к CSV файлу

        Returns:
            DataFrame с данными из CSV
        """
        print(f"Извлечение данных из CSV: {file_path}")
        try:
            df = pd.read_csv(file_path)
            print(f"Извлечено {len(df)} записей из CSV")
            return df
        except FileNotFoundError:
            print(f"Файл не найден: {file_path}")
            return pd.DataFrame()
        except Exception as e:
            print(f"Ошибка при чтении CSV: {e}")
            return pd.DataFrame()

    def extract_from_api(self, api_url, params=None):
        """
        Дополнительный метод: извлечение данных из внешнего API

        Args:
            api_url: URL API endpoint
            params: параметры запроса (optional)

        Returns:
            DataFrame с данными из API
        """
        import requests

        print(f"Извлечение данных из API: {api_url}")
        try:
            response = requests.get(api_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            df = pd.DataFrame(data)
            print(f"Извлечено {len(df)} записей из API")
            return df
        except Exception as e:
            print(f"Ошибка при запросе к API: {e}")
            return pd.DataFrame()
