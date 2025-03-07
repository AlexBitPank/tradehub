from os import getenv
import time
import hmac
import hashlib
import requests
from urllib.parse import urlencode


API_KEY = getenv("API_KEY_BIN")
API_SECRET = getenv("SECRET_KEY_BIN")


BASE_URL = 'https://fapi.binance.com'  # Для торговли на USDS-маржинальных фьючерсах

def sign(params, secret):
    """
    Функция для подписи запроса (HMAC SHA256).
    params: словарь с параметрами запроса
    secret: ваш Secret Key
    """
    query_string = urlencode(params)
    signature = hmac.new(
        secret.encode('utf-8'), 
        query_string.encode('utf-8'), 
        hashlib.sha256
    ).hexdigest()
    return signature

def new_order(symbol, side, positionSide, order_type, quantity, price=None, stop_price=None, time_in_force=None):
    """
    Функция для создания нового ордера на фьючерсном рынке Binance.
    
    Параметры:
      symbol (str): Символ для торговли, например 'BTCUSDT'
      side (str): BUY или SELL
      order_type (str): Тип ордера: MARKET, LIMIT, STOP, STOP_MARKET, TAKE_PROFIT и т.д.
      quantity (float): Количество (в контрактах)
      price (float): Цена (только для ордеров с лимитом, опционально)
      stop_price (float): Стоп-цена (опционально)
      time_in_force (str): GTC, IOC, FOK (опционально)
    """
    # Эндпоинт для отправки ордера
    endpoint = '/fapi/v1/order'
    
    # Текущее время в миллисекундах (необходимо для отправки вместе с запросом)
    timestamp = int(time.time() * 1000)
    
    # Параметры для тела запроса
    params = {
        'symbol': symbol,
        'side': side.upper(),
        'positionSide': positionSide,
        'type': order_type.upper(),
        'quantity': quantity,
        'timestamp': timestamp,
    }
    
    # Если у нас лимитный ордер, добавим цену и timeInForce
    if order_type.upper() == 'LIMIT':
        if price is None:
            raise ValueError("Для LIMIT-ордера параметр price обязателен.")
        if not time_in_force:
            time_in_force = 'GTC'
        params['price'] = price
        params['timeInForce'] = time_in_force

    # Если это ордер со стоп-ценой (STOP или TAKE_PROFIT и т.д.)
    if stop_price is not None:
        params['stopPrice'] = stop_price

    # Подпишем запрос
    params['signature'] = sign(params, API_SECRET)

    # Заголовки, включая API-ключ
    headers = {
        'X-MBX-APIKEY': API_KEY
    }
    
    # Выполним POST-запрос
    url = BASE_URL + endpoint
    response = requests.post(url, params=params, headers=headers)
    headers = response.headers
    for k, v in headers.items():
        print(k,v)
    # print(f"Использовано за запрос: {headers.get('x-mbx-used-weight')}")
    # print(f"Использовано за минуту: {headers.get('x-mbx-used-weight-1m')}")
    
    # Проверим результат
    if response.status_code != 200:
        print(f"Ошибка. Код ответа: {response.status_code}, Тело: {response.text}")
    else:
        print("Ордер создан успешно.")
    
    return response.json()

if __name__ == '__main__':
    # Пример: рыночная покупка 0.001 BTC в паре BTCUSDT
    result = new_order(
        symbol='ADAUSDT',
        side='BUY',
        positionSide='LONG',
        order_type='MARKET',
        quantity=10
    )

    # print(result)
