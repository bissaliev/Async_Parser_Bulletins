# Асинхронный парсер бюллетеней

Асинхронное скачивание файлов с сайта биржи (<https://spimex.com/markets/oil_products/trades/results/>), а также последующий парсинг и загрузку данных в БД (PostgreSQL).

## Стек

- `Python 3.12`
- `PostgreSQL 15`
- `BeautifulSoup 4`
- `SQLAlchemy 2.0(asyncpg)`
- `Pandas 2.2`
- `aiohttp 3.11`

## Структура приложения

- `/app/database/` - Директория конфигураций БД
  - `database.py` - Настройки подключений к БД
  - `models.py` - Содержит модель `SpimexTradingResults`
  - `crud.py` - Содержит функцию вставки данных в таблицу `spimex_trading_results`
- `/app/parsers/` - Директория запросов и парсинга
  - `parser.py` - Содержит класс `Parser`, который извлекает ссылки со страницы html
  - `scraper.py` - Содержит функции `fetch_page`(Получение страницы) и `fetch_file`(Получение файла)
- `/app/utils/` - Директория обработки данных `*.xls`
  - `file_utils.py` - Содержит класс `XLSExtractor`, который извлекает и отдает нужные данные
- `/app/config.py` - Основные настройки проекта
- `/app/logging_config.py` - Конфигурации логирования
- `/app/main.py` - Главный модуль
- `.env.example` - Образец файла переменных окружения
- `/app/init_db.py` - Создает таблицу `spimex_trading_results` в БД
- `/app/exceptions.py` - Кастомные классы исключения

## Запуск

- Клонируйте репозиторий:

    ```bash
    git clone git@github.com:bissaliev/Async_Parser_Bulletins.git
    ```

- Создайте файл для переменных окружения(по образцу `.env.example`)

- Запустите докер-контейнер для `PostgreSQL` командой:

    ```bash
    docker run --name db_async_bulletins --volume bul_data:/var/lib/postgresql/data --env-file ./.env -p 5432:5432 postgres
    ```

- Перейдите в директорию `/app` и выполните инициализацию создания таблиц в БД:

    ```bash
    init_db.py
    ```

- Запустите модуль `main.py`:

    ```bash
    python3 main.py
    ```

## Сравнительная оценка скорости выполнения синхронного и асинхронного кода

### Синхронный код

Время выполнения синхронного кода: `~557.5` секунд (`~9` минут)

### Асинхронный код

Время выполнения асинхронного кода: `~45.1` секунд (`~0.75` минуты)

### Анализ

Асинхронный код в `~12` раз быстрее, чем синхронный.
