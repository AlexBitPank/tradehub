import requests

# Эндпоинт для открытого интереса (USDT-M Futures)
url = "https://fapi.binance.com/futures/data/openInterestHist"

# Параметры запроса
params = {
    "symbol": "SUIUSDT",  # Нужный нам торговый инструмент
    "period": "5m",       # Период статистики (доступны 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d)
    "limit": 10           # Количество записей в ответе (максимум 500)
    # "startTime": <время в мс>,
    # "endTime":   <время в мс>,
}

response = requests.get(url, params=params)

# Если всё хорошо, то status_code == 200
if response.status_code == 200:
    data = response.json()
    print("Open Interest для SUIUSDT:")
    for item in data:
        print(item)
else:
    print(f"Ошибка при получении данных: {response.status_code}")
    print(response.text)
