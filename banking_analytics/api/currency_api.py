# api/currency_api.py
import requests
import pandas as pd
from datetime import datetime


class CurrencyAPI:
    """Работа с API курсов валют (используем ЦБ РФ API)"""

    def __init__(self):
        self.base_url = "https://www.cbr-xml-daily.ru/daily_json.js"

    def get_exchange_rates(self):
        """Получение текущих курсов валют через API"""
        try:
            print("Получение курсов валют от ЦБ РФ...")
            response = requests.get(self.base_url, timeout=10)
            response.raise_for_status()
            data = response.json()

            rates = {
                'date': datetime.now().date(),
                'usd_to_rub': data['Valute']['USD']['Value'],
                'eur_to_rub': data['Valute']['EUR']['Value'],
                'usd_to_eur': data['Valute']['USD']['Value'] / data['Valute']['EUR']['Value']
            }

            print(f"✓ Курсы получены: USD={rates['usd_to_rub']:.2f} RUB, EUR={rates['eur_to_rub']:.2f} RUB")
            return pd.DataFrame([rates])

        except requests.exceptions.RequestException as e:
            print(f"⚠ Ошибка при получении курсов валют: {e}")
            print("Используем дефолтные значения...")
            return pd.DataFrame([{
                'date': datetime.now().date(),
                'usd_to_rub': 90.0,
                'eur_to_rub': 100.0,
                'usd_to_eur': 0.9
            }])
