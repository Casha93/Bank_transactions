# etl/transform.py
import pandas as pd
import numpy as np
from datetime import datetime


class DataTransformer:
    """Обработка и обогащение данных в Pandas"""

    def __init__(self, exchange_rates_df):
        self.exchange_rates = exchange_rates_df

    def clean_customers(self, df):
        """Очистка и обогащение данных клиентов"""
        # Удаление дубликатов
        df = df.drop_duplicates(subset=['customer_id'])

        # Обработка пропущенных значений
        df['phone'] = df['phone'].fillna('Unknown')
        df['email'] = df['email'].str.lower().str.strip()

        # Создание полного имени
        df['full_name'] = df['first_name'] + ' ' + df['last_name']

        # ИСПРАВЛЕНО: Расчет возраста
        # Конвертируем обе даты в pandas datetime
        today = pd.to_datetime(datetime.now().date())
        birth_dates = pd.to_datetime(df['date_of_birth'])

        # Вычисляем возраст
        df['age'] = ((today - birth_dates).dt.days // 365).astype(int)

        # Категоризация по возрасту
        df['age_group'] = pd.cut(df['age'],
                                 bins=[0, 25, 35, 45, 55, 100],
                                 labels=['18-25', '26-35', '36-45', '46-55', '55+'])

        return df

    def clean_transactions(self, df):
        """Очистка транзакций"""
        # Удаление незавершенных транзакций для анализа
        df = df[df['transaction_status'] == 'Completed'].copy()

        # Преобразование типов
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        df['amount'] = df['amount'].abs()  # Только положительные суммы

        # Удаление выбросов (транзакции > 1 млн)
        df = df[df['amount'] < 1000000]

        return df

    def enrich_with_currency_rates(self, transactions_df):
        """Обогащение данных курсами валют через API"""
        # Получаем курсы валют
        rates = self.exchange_rates.iloc[0]

        def convert_to_rub(row):
            """Конвертация всех валют в рубли"""
            if row['currency'] == 'RUB':
                return row['amount'], 1.0
            elif row['currency'] == 'USD':
                return row['amount'] * rates['usd_to_rub'], rates['usd_to_rub']
            elif row['currency'] == 'EUR':
                return row['amount'] * rates['eur_to_rub'], rates['eur_to_rub']
            return row['amount'], 1.0

        # ИСПРАВЛЕНО: Используем apply с правильным типом результата
        result = transactions_df.apply(convert_to_rub, axis=1, result_type='expand')
        transactions_df['amount_rub'] = result[0]
        transactions_df['exchange_rate'] = result[1]

        return transactions_df

    def create_date_dimension(self, start_date, end_date):
        """Создание измерения дат"""
        dates = pd.date_range(start=start_date, end=end_date, freq='D')

        date_dim = pd.DataFrame({
            'date_key': dates.strftime('%Y%m%d').astype(int),
            'date': dates,
            'year': dates.year,
            'quarter': dates.quarter,
            'month': dates.month,
            'month_name': dates.strftime('%B'),
            'week': dates.isocalendar().week.astype(int),
            'day_of_month': dates.day,
            'day_of_week': dates.dayofweek + 1,
            'day_name': dates.strftime('%A'),
            'is_weekend': dates.dayofweek.isin([5, 6])
        })

        return date_dim

    def aggregate_transaction_metrics(self, transactions_df):
        """Агрегация метрик для анализа"""
        metrics = transactions_df.groupby(['account_id', 'transaction_type']).agg({
            'amount_rub': ['sum', 'mean', 'count'],
            'transaction_id': 'count'
        }).reset_index()

        return metrics
