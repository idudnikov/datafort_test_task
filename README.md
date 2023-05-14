# Тестовое задание для компании DataFort

## Подготовка и запуск проекта
### Склонируйте репозиторий на локальную машину
    git clone https://github.com/idudnikov/datafort_test_task.git
### Cоздайте .env файл в директории weather_collector и впишите в него API клю
    API_KEY=<API ключ сайта "https://openweathermap.org/">
#### Для целей тестирования можно использовать мой API ключ
    b7c12c9ba2e4c1efe6b6799a45a2822e
### Соберите и запустите Docker контейнер
    docker-compose up -d --build
### Коллектор и запущен и теперь собирает данные о погоде в 50 крупнейших городах каждый час

## Комментарии
Для реализации ТЗ была выбрана следующая структура БД:
- Таблица с данными о городах, стране принадлежности, населению и географическим координатам.
Так как информация о величине населенгия городоы обновляются нечасто, то данные о 50 крупнейших города забираются один
раз с API opendatasoft.com, записываются в БД и затем используются для получения данных о состоянии погоды.
- Таблица с состояниями погоды.
API openweathermap.org среди данных о погоде в конкретном городе отдает в составе данных информацию о состоянии погоды
(ясное небо, гроза, легкая облачность, небольшой дождь и так далее). Так как эти данные используются несколько раз для
данных о погоде из разных городов, то их логично хранить в отдельной таблице.
- Таблица с данными о погоде.
Структура этой таблицы выбрана в соответствии с тем объемом данных, которые отдает API opendatasoft.com. Не для всех
данных очевидно применение, но они все записываются в БД для возможности использования в будущем. Некоторые данные,
такие как объем осадков за последний час или 3 часа, могу отсутствовать в данных. В этому случае вместо них в базу
данных записывается NULL.

Технологии:
- На данном этапе выбрана простая СУБД SQLite, так как ее ресурса хватает для записи 50 строк данных раз в час. При
увеличении нагрузки на БД необходимо перейти на более производительную БД, например PostgreSQL.