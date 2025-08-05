import requests
from datetime import datetime, timedelta, timezone
import pytz
import json
import csv

# Extract (загрузка данных)
def fetch_raw_data():
    """Загружает данные из API Open-Meteo"""
    url = "https://api.open-meteo.com/v1/forecast?latitude=55.0344&longitude=82.9434&daily=sunrise,sunset,daylight_duration&hourly=temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,temperature_80m,temperature_120m,wind_speed_10m,wind_speed_80m,wind_direction_10m,wind_direction_80m,visibility,evapotranspiration,weather_code,soil_temperature_0cm,soil_temperature_6cm,rain,showers,snowfall&timezone=auto&timeformat=unixtime&wind_speed_unit=kn&temperature_unit=fahrenheit&precipitation_unit=inch&start_date=2025-05-16&end_date=2025-05-30"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API Error: {response.status_code}")

# Transform (преобразование данных)
def convert_units(raw_data):
    data_converted = raw_data.copy()

    # продолжительность светового дня в часах
    if 'daily' in data_converted:
        if 'daylight_duration' in data_converted['daily']:
            data_converted['daily']['daylight_hours'] = [duration / 3600 for duration in data_converted['daily']['daylight_duration']]

    # дата в часовом поясе Новосибирска
    daily_data = data_converted['daily']

    temp_keys = [
         'time',
         'sunrise',
         'sunset'
    ]
    novosibirsk_tz = pytz.timezone('Asia/Novosibirsk')
    for key in temp_keys:
        if key in daily_data:
            daily_data[key] = [datetime.fromtimestamp(timestamp, tz=timezone.utc) for timestamp in daily_data[key]]
            daily_data[key] = [utc.astimezone(novosibirsk_tz) for utc in daily_data[key]]

    # выделение даты
    if 'time' in daily_data:
        daily_data['date'] = [time.date() for time in daily_data['time']]

    # температура
    hourly_data = data_converted['hourly']

    temp_keys = [
        'temperature_2m',
        'dew_point_2m',
        'apparent_temperature',
        'temperature_80m',
        'temperature_120m',
        'soil_temperature_0cm',
        'soil_temperature_6cm'
    ]

    for key in temp_keys:
        if key in hourly_data:
            hourly_data[key] = [(f - 32) * 5 / 9 if f is not None else None for f in hourly_data[key]]

    # скорость ветра
    speed_keys = ['wind_speed_10m','wind_speed_80m']
    for key in speed_keys:
        if key in hourly_data:
            hourly_data[key] = [kn * 0.51444 for kn in hourly_data[key]]

    # осадки
    inch_keys = ['rain', 'showers', 'snowfall']
    for key in inch_keys:
        if key in hourly_data:
            hourly_data[key] = [inch * 25.4 for inch in hourly_data[key]]

    # видимость
    if 'visibility' in hourly_data:
        hourly_data['visibility'] = [ft * 0.3048 for ft in hourly_data['visibility']]

    # дата в часовом поясе Новосибирска
    if 'time' in hourly_data:
        hourly_data['time'] = [datetime.fromtimestamp(timestamp, tz=timezone.utc) for timestamp in hourly_data['time']]
        hourly_data['time'] = [utc.astimezone(novosibirsk_tz) for utc in hourly_data['time']]

    # выделение даты
    if 'time' in hourly_data:
        hourly_data['date'] = [time.date() for time in hourly_data['time']]

    # Словарь с восходами/закатами по датам
    daily_lookup = {}
    for i in range(len(daily_data['date'])):
        date = daily_data['date'][i]
        daily_lookup[date] = {
            'sunrise': daily_data['sunrise'][i],
            'sunset': daily_data['sunset'][i]
        }

    # Добавление данные о восходе/закате
    hourly_data['sunrise'] = []
    hourly_data['sunset'] = []

    for date in hourly_data['date']:
        if date in daily_lookup:
            hourly_data['sunrise'].append(daily_lookup[date]['sunrise'])
            hourly_data['sunset'].append(daily_lookup[date]['sunset'])

    # флаг светового дня

    if 'time' in hourly_data and 'sunrise' in hourly_data and 'sunset' in hourly_data:
        hourly_data['is_daylight'] = [sunrise <= time <= sunset  for time, sunrise, sunset
                                      in zip(hourly_data['time'], hourly_data['sunrise'], hourly_data['sunset'])]

    return data_converted


def calculate_daily_aggregates(data_converted):
    daily_data = data_converted['daily']
    hourly_data = data_converted['hourly']

    # Словарь агрегации
    agg_rules = {
        'temperature_2m': 'mean',
        'relative_humidity_2m': 'mean',
        'dew_point_2m': 'mean',
        'apparent_temperature': 'mean',
        'temperature_80m': 'mean',
        'temperature_120m': 'mean',
        'wind_speed_10m': 'mean',
        'wind_speed_80m': 'mean',
        'visibility': 'mean',
        'rain': 'sum',
        'showers': 'sum',
        'snowfall': 'sum'
    }

    # Время восхода и заката для каждой даты
    sunrise_sunset = {}
    for i in range(len(daily_data['date'])):
        date = daily_data['date'][i]
        sunrise_sunset[date] = {
            'sunrise': daily_data['sunrise'][i],
            'sunset': daily_data['sunset'][i]
        }

    # Все данные по датам
    date_stats = {}
    for date in daily_data['date']:
        date_stats[date] = {
            'all': {metric: [] for metric in agg_rules},
            'day': {metric: [] for metric in agg_rules}
        }

    # Почасовые данные
    for i in range(len(hourly_data['date'])):
        date = hourly_data['date'][i]
        time = hourly_data['time'][i]
        sunrise = sunrise_sunset[date]['sunrise']
        sunset = sunrise_sunset[date]['sunset']

        for metric in agg_rules:
            if metric in hourly_data:
                value = hourly_data[metric][i]
                date_stats[date]['all'][metric].append(value)
                if sunrise <= time <= sunset:
                    date_stats[date]['day'][metric].append(value)

    # Результаты в daily_data
    for metric, rule in agg_rules.items():
        # Префикс в зависимости от типа расчета
        prefix = 'avg' if rule == 'mean' else 'total'

        # Для всех суток
        daily_data[f'{prefix}_{metric}_24h'] = [
            _calculate(date_stats[date]['all'].get(metric, []), rule)
            for date in daily_data['date']
        ]

        # Для светового дня
        daily_data[f'{prefix}_{metric}_daylight'] = [
            _calculate(date_stats[date]['day'].get(metric, []), rule)
            for date in daily_data['date']
        ]

    return daily_data

def _calculate(values, rule):
    """Считает среднее или сумму"""
    if not values:  # Если нет данных
        return None
    if rule == 'mean':
        return sum(values) / len(values)
    elif rule == 'sum':
        return sum(values)
    return None

def rename_hourly_keys(hourly_data):
    # Словарь для результата
    renamed_data = {}

    for key, values in hourly_data.items():
        # Добавление _celsius к температурам
        if 'temperature' in key.lower() or 'dew_point' in key.lower():
            new_key = f"{key}_celsius"
        # Добавление _mm к осадкам
        elif key in ['rain', 'showers', 'snowfall']:
            new_key = f"{key}_mm"
        # Добавление _m_per_s к скорости
        elif 'speed' in key.lower():
            new_key = f"{key}_m_per_s"
        else:
            new_key = key

        renamed_data[new_key] = values

    return renamed_data



def export_daily_to_csv(daily_data, filename):
    # Порядок столбцов
    fieldnames = [
        'time',

        # Суточные агрегаты (24h)
        'avg_temperature_2m_24h',
        'avg_relative_humidity_2m_24h',
        'avg_dew_point_2m_24h',
        'avg_apparent_temperature_24h',
        'avg_temperature_80m_24h',
        'avg_temperature_120m_24h',
        'avg_wind_speed_10m_24h',
        'avg_wind_speed_80m_24h',
        'avg_visibility_24h',
        'total_rain_24h',
        'total_showers_24h',
        'total_snowfall_24h',

        # Агрегаты за световой день
        'avg_temperature_2m_daylight',
        'avg_relative_humidity_2m_daylight',
        'avg_dew_point_2m_daylight',
        'avg_apparent_temperature_daylight',
        'avg_temperature_80m_daylight',
        'avg_temperature_120m_daylight',
        'avg_wind_speed_10m_daylight',
        'avg_wind_speed_80m_daylight',
        'avg_visibility_daylight',
        'total_rain_daylight',
        'total_showers_daylight',
        'total_snowfall_daylight',

        # Время восхода/заката
        'daylight_hours',
        'sunrise_iso',
        'sunset_iso'
    ]


    # Подготовка данных для записи
    rows = []
    for i in range(len(daily_data['time'])):
      row = {
      'sunrise_iso': daily_data['sunrise'][i].isoformat(),
      'sunset_iso': daily_data['sunset'][i].isoformat()
      }

      for field in fieldnames:
        if field not in ['sunrise_iso', 'sunset_iso']:
          row[field] = daily_data[field][i]

      rows.append(row)

    # Запись в CSV
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def export_hourly_to_csv(hourly_data, filename):
    # Порядок столбцов
    fieldnames = [
        'time',
        'wind_speed_10m_m_per_s',
        'wind_speed_80m_m_per_s',
        'temperature_2m_celsius',
        'apparent_temperature_celsius',
        'temperature_80m_celsius',
        'temperature_120m_celsius',
        'soil_temperature_0cm_celsius',
        'soil_temperature_6cm_celsius',
        'rain_mm',
        'showers_mm',
        'snowfall_mm'
    ]

    # Подготовка данных для записи
    rows = []
    for i in range(len(hourly_data['time'])):
        row = {}

        for field in fieldnames:
            row[field] = hourly_data[field][i]

        rows.append(row)

    # Запись в CSV
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    try:

        # Extract - загрузка данных
        raw_data = fetch_raw_data()

        # Transform - преобразование данных
        data_converted = convert_units(raw_data)

        # Расчет агрегатов
        daily_data = calculate_daily_aggregates({'hourly': data_converted['hourly'], 'daily': data_converted['daily']})

        # Переименование ключей почасовых данных
        hourly_data = rename_hourly_keys(data_converted['hourly'])

        # 5. Экспорт в CSV
        export_hourly_to_csv(hourly_data, 'hourly_weather_report.csv')
        export_daily_to_csv(daily_data, 'daily_weather_report.csv')

        return True

    except Exception as e:
        print(f"\n!!! Ошибка: {e}")
        print("=== Обработка прервана ===")

if __name__ == "__main__":
    main()