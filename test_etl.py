import csv
import os
from weather_etl_script import main, fetch_raw_data
import pytest

# Фикстура - запускает ETL один раз перед всеми тестами
@pytest.fixture(scope="module", autouse=True)
def run_etl_once():
    """Запускает ETL процесс перед всеми тестами"""
    if not main():
        pytest.fail("ETL process failed")

def test_files_created():
    """Проверяем, что CSV-файлы созданы"""
    assert os.path.exists('hourly_weather_report.csv')
    assert os.path.exists('daily_weather_report.csv')

def test_hourly_columns():
    """Тест колонок в hourly отчете"""
    expected = {
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
    }

    with open('hourly_weather_report.csv', 'r') as f:
        reader = csv.DictReader(f)
        assert set(reader.fieldnames) == expected, (
            f"Несовпадение колонок. Ожидалось: {expected}, "
            f"Получено: {set(reader.fieldnames)}"
        )


def test_daily_columns():
    """Тест колонок в daily отчете"""
    expected = {
        'time',
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
        'daylight_hours',
        'sunrise_iso',
        'sunset_iso'
    }

    with open('daily_weather_report.csv', 'r') as f:
        reader = csv.DictReader(f)
        assert set(reader.fieldnames) == expected, (
            f"Несовпадение колонок. Ожидалось: {expected}, "
            f"Получено: {set(reader.fieldnames)}"
        )

def test_data_integrity():
    """Проверяем, что данные не пустые"""
    with open('daily_weather_report.csv', 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) > 0
        for row in rows:
            assert row['avg_temperature_2m_24h'] is not None