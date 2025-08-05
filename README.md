# Парсер погодных данных

Приложение для выгрузки данных прогнозов погоды с Open-Meteo API

## Как использовать
1. Установите зависимости:
```bash
pip install -r requirements.txt
```

2. Запустите основной скрипт:
```bash
python weather_etl_script.py
```

3. Запустите тесты:
```bash
pytest test_etl.py -v
```

## Структура проекта
- `weather_etl_script.py` — основной ETL-скрипт
- `test_etl.py` — тесты
