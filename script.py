import os
import sqlite3
import time
from datetime import datetime as dt

import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")


def create_db() -> None:
    """
    Функция для первоначального создания базы данных.
    :return: None
    """

    commands = (
        """
        CREATE TABLE IF NOT EXISTS cities (
            city_id INTEGER PRIMARY KEY NOT NULL,
            name VARCHAR(100) UNIQUE NOT NULL,
            country VARCHAR(100) NOT NULL,
            population INT(10) NOT NULL
        );        
        """,
        """
        CREATE TABLE IF NOT EXISTS weather_condition (
            weather_id INTEGER PRIMARY KEY NOT NULL,
            description VARCHAR(100) NOT NULL,
            icon_id VARCHAR(10) NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS current_weather (
            cw_id INTEGER PRIMARY KEY NOT NULL,
            city_id INT REFERENCES cities (city_id) ON UPDATE CASCADE,
            weather_id INT REFERENCES weather_condition (weather_id) ON UPDATE CASCADE,
            temperature FLOAT,
            temperature_feels_like FLOAT,
            min_temperature FLOAT,
            max_temperature FLOAT,
            humidity INT,
            pressure INT,
            sea_level_pressure INT,
            ground_level_pressure INT,
            visibility INT,
            wind_speed FLOAT,
            wind_direction INT,
            wind_gust FLOAT,
            clouds INT,
            rain_1h FLOAT,
            rain_3h FLOAT,
            snow_1h FLOAT,
            snow_3h FLOAT,
            date_time TIMESTAMP,
            sunrise TIMESTAMP,
            sunset TIMESTAMP
        );
        """,
    )

    sqlite_connection = None
    try:
        sqlite_connection = sqlite3.connect("weather_data.db")
        cursor = sqlite_connection.cursor()
        for command in commands:
            cursor.execute(command)
        cursor.close()
        sqlite_connection.commit()
    except sqlite3.Error as error:
        print(error)
    else:
        print(f'{dt.now().strftime("%d.%m.%Y %H:%M:%S")}. База данных успешно создана.')
    finally:
        if sqlite_connection is not None:
            sqlite_connection.close()

    return


def get_top_50_cities_by_population() -> list:
    """
    Функция для получения списка топ 50 городов по населению из API 'opendatasoft.com', обработки и сохранения
    данных в отдельный json файл для дальнейшего использования.
    :return: Список 50 городов.
    """

    cities_count = 50
    url = f"https://data.opendatasoft.com/api/records/1.0/search/?dataset=geonames-all-cities-with-a-population-1000%40public&q=&lang=en&rows={cities_count}&sort=population"
    r = requests.get(url)
    initial_data = r.json()["records"]
    final_data = []
    for index, element in enumerate(initial_data):
        interim_data = {
            "id": index + 1,
            "city_name": element["fields"]["ascii_name"],
            "country_name": element["fields"]["cou_name_en"],
            "population": element["fields"]["population"],
            "latitude": element["fields"]["coordinates"][0],
            "longitude": element["fields"]["coordinates"][1],
        }
        final_data.append(interim_data)

    return final_data


def add_cities_to_db(cities_data: list) -> None:
    """
    Функция для добавления в БД данных о 50 крупнейших городах.
    :param cities_data: Данные о городах для добавления в БД.
    :return: None
    """

    sqlite_connection = None
    try:
        sqlite_connection = sqlite3.connect("weather_data.db")
        cursor = sqlite_connection.cursor()
        for city_data in cities_data:
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO cities (city_id, name, country, population)
                VALUES (?, ?, ?, ?);
                """,
                (
                    city_data["id"],
                    city_data["city_name"],
                    city_data["country_name"],
                    city_data["population"],
                ),
            )

        cursor.close()
        sqlite_connection.commit()
    except sqlite3.Error as error:
        print(error)
    else:
        print(
            f'{dt.now().strftime("%d.%m.%Y %H:%M:%S")}. Данные о городах успешно добавлены в базу данных.'
        )
    finally:
        if sqlite_connection is not None:
            sqlite_connection.close()

    return


def get_weather_info(lat, lon) -> dict:
    """
    Функция для получения погоды в конкретном городе по географическим координатам города.
    :param lat: координата широты города
    :param lon: координата долготы города
    :return: Данные о погоде в конкретном города в формате dict.
    """

    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
    r = requests.get(url)

    return r.json()


def parse_weather_info(cities_data: list) -> [list, list]:
    """
    Функция для получения из API 'openweathermap.org' данных о текущей погоде и парсинга полученных данных.
    :param cities_data: данные о 50 крупнейших городах.
    :return: список с данными о погодных условиях и список с данными о текущей погоде в городах.
    """

    weather_condition_dataset = []
    current_weather_dataset = []
    for city_data in cities_data:
        weather_info = get_weather_info(city_data["latitude"], city_data["longitude"])
        weather_condition_data = {
            "id": weather_info["weather"][0]["id"],
            "description": weather_info["weather"][0]["description"],
            "icon_id": weather_info["weather"][0]["icon"],
        }
        weather_condition_dataset.append(weather_condition_data)

        current_weather_data = {
            "city_id": city_data["id"],
            "weather_id": weather_condition_data["id"],
            "temperature": weather_info["main"]["temp"],
            "temperature_feels_like": weather_info["main"]["feels_like"],
            "min_temperature": weather_info["main"]["temp_min"],
            "max_temperature": weather_info["main"]["temp_max"],
            "humidity": weather_info["main"]["humidity"],
            "pressure": weather_info["main"]["pressure"],
            "sea_level_pressure": weather_info["main"].get("sea_level"),
            "ground_level_pressure": weather_info["main"].get("grnd_level"),
            "visibility": weather_info["visibility"],
            "wind_speed": weather_info["wind"]["speed"],
            "wind_direction": weather_info["wind"]["deg"],
            "wind_gust": weather_info["wind"].get("gust"),
            "clouds": weather_info["clouds"]["all"],
            "rain_1h": None
            if weather_info.get("rain") is None
            or weather_info["rain"].get("1h") is None
            else weather_info["rain"]["1h"],
            "rain_3h": None
            if weather_info.get("rain") is None
            or weather_info["rain"].get("3h") is None
            else weather_info["rain"]["3h"],
            "snow_1h": None
            if weather_info.get("snow") is None
            or weather_info["snow"].get("1h") is None
            else weather_info["snow"]["1h"],
            "snow_3h": None
            if weather_info.get("snow") is None
            or weather_info["snow"].get("3h") is None
            else weather_info["snow"]["3h"],
            "datetime": weather_info["dt"],
            "sunrise": weather_info["sys"]["sunrise"],
            "sunset": weather_info["sys"]["sunset"],
        }
        current_weather_dataset.append(current_weather_data)

    return weather_condition_dataset, current_weather_dataset


def add_weather_condition_to_db(data: list) -> None:
    """
    Функция для добавления в базу данных информации о статусах погоды.
    :param data: Список статусов погоды для добавления в БД.
    :return: None
    """

    sqlite_connection = None
    try:
        sqlite_connection = sqlite3.connect("weather_data.db")
        cursor = sqlite_connection.cursor()
        for element in data:
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO weather_condition (weather_id, description, icon_id)
                VALUES (?, ?, ?);
                """,
                (element["id"], element["description"], element["icon_id"]),
            )
        cursor.close()
        sqlite_connection.commit()
    except sqlite3.Error as error:
        print(error)
    else:
        print(
            f'{dt.now().strftime("%d.%m.%Y %H:%M:%S")}. Данные о статусах погоды успешно добавлены в базу данных.'
        )
    finally:
        if sqlite_connection is not None:
            sqlite_connection.close()

    return


def add_current_weather_to_db(data: list) -> None:
    """
    Функция для добавления в базу данных информации о текущих погодных условиях в городах.
    :param data: Данны о погодных условиях в городах для добавления в БД.
    :return: None
    """

    sqlite_connection = None
    try:
        sqlite_connection = sqlite3.connect("weather_data.db")
        cursor = sqlite_connection.cursor()
        for element in data:
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO current_weather (city_id, weather_id, temperature, temperature_feels_like,
                min_temperature, max_temperature, humidity, pressure, sea_level_pressure, ground_level_pressure,
                visibility, wind_speed, wind_direction, wind_gust, clouds, rain_1h, rain_3h, snow_1h, snow_3h,
                date_time, sunrise, sunset)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    element["city_id"],
                    element["weather_id"],
                    element["temperature"],
                    element["temperature_feels_like"],
                    element["min_temperature"],
                    element["max_temperature"],
                    element["humidity"],
                    element["pressure"],
                    element["sea_level_pressure"],
                    element["ground_level_pressure"],
                    element["visibility"],
                    element["wind_speed"],
                    element["wind_direction"],
                    element["wind_gust"],
                    element["clouds"],
                    element["rain_1h"],
                    element["rain_3h"],
                    element["snow_1h"],
                    element["snow_3h"],
                    element["datetime"],
                    element["sunrise"],
                    element["sunset"],
                ),
            )
        cursor.close()
        sqlite_connection.commit()
    except sqlite3.Error as error:
        print(error)
    else:
        print(
            f'{dt.now().strftime("%d.%m.%Y %H:%M:%S")}. Данные о текущей погоде успешно добавлены в базу данных.'
        )
    finally:
        if sqlite_connection is not None:
            sqlite_connection.close()

    return


if __name__ == "__main__":
    # Создаем базу данных.
    create_db()

    # Получаем данные о 50 крупнейших городах.
    cities_data = get_top_50_cities_by_population()

    # Добавляем данные о городах в БД.
    add_cities_to_db(cities_data)

    while True:
        # 1. Получаем данные о погодных условиях в городах и парсим данные.
        weather_condition_dataset, current_weather_dataset = parse_weather_info(
            cities_data
        )

        # 2. Добавляем данные о статусах погоды в БД.
        add_weather_condition_to_db(weather_condition_dataset)

        # 3. Добавляем данные о текущих погодных условиях в БД.
        add_current_weather_to_db(current_weather_dataset)

        # Ожидаем 1 час и повторяем шаги 1-3.
        time.sleep(20)
