import asyncio
import io
import time
from datetime import date, datetime

import aiohttp
from database.crud import mass_create_trade
from logging_config import logger
from parsers.parser import Parser
from utils.file_utils import XLSExtractor

BASE_URL = "https://spimex.com"
PAGE_URL = BASE_URL + "/markets/oil_products/trades/results/"
CURRENT_YEAR = datetime.now().year
MIN_YEAR = 2023
FIRST_PAGE = 1
LAST_PAGE = 384
MAX_CONCURRENT_REQUESTS = 15  # Максимальное число одновременных запросов


async def fetch_file(session: aiohttp.ClientSession, url: str):
    """Скачиваем файл и отдаем контент"""
    try:
        async with session.get(url, raise_for_status=True) as response:
            return await response.read()
    except aiohttp.ClientResponseError as e:
        logger.error(f"Ошибка при скачивание страницы файла: {e}")
    except aiohttp.ClientError as e:
        logger.error(f"Ошибка при скачивание файла: {e}")


async def download_data(session: aiohttp.ClientSession, url: str, bidding_date: date):
    """Скачивает файл, обрабатывает его и сохраняет данные в БД"""

    content = await fetch_file(session, url)

    # Создаем класс извлекающий нужные данные из файла xls
    xls_extractor = XLSExtractor(io.BytesIO(content), bidding_date)
    data = xls_extractor.get_data()

    # Сохраняем данные в БД
    await mass_create_trade(data)


async def fetch(session: aiohttp.ClientSession, url: str, params=None):
    """Запрашиваем страницу и отдаем html-страницу"""
    try:
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            logger.info(f"Страница {response.url} загружена")
            return await response.text()
    except aiohttp.ClientResponseError as e:
        logger.error(f"Ошибка при скачивание страницы {params['page']}: {e.status}")
    except aiohttp.ClientError as e:
        logger.error(f"Ошибка при скачивание страницы {params['page']}: {e}")


async def process_page(session: aiohttp.ClientSession, page: int):
    """Обрабатывает одну страницу: парсит ссылки и загружает файлы"""
    html = await fetch(session, PAGE_URL, params={"page": f"page-{page}"})
    if html is None:
        logger.warning(f"Пропускаем страницу {page}, так как HTML не был загружен")
        return
    parser = Parser(html, MIN_YEAR, CURRENT_YEAR)
    file_links = parser.extract_file_links()
    tasks = []
    for link, bidding_date in file_links:
        tasks.append(asyncio.create_task(download_data(session, BASE_URL + link, bidding_date)))
    await asyncio.gather(*tasks)
    logger.info(f"Страница {page} загружена")


async def main():
    """Главный модуль"""
    # В цикле проходимся по страницам со ссылка на файлы
    tasks = []
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        for page in range(FIRST_PAGE, LAST_PAGE + 1):
            tasks.append(asyncio.create_task(process_page(session, page)))
        try:
            await asyncio.gather(*tasks)
            logger.info("Загрузка завершена")
        except Exception as e:
            logger.error(f"Произошла ошибка: {e}")


if __name__ == "__main__":
    start_time = time.perf_counter()
    asyncio.run(main())
    end_time = time.perf_counter()
    print(f"Время выполнения: {end_time - start_time}")


# Время выполнения (5 страниц) 11.814728350000223

# Время выполнения с использованием to_thread (5 страниц): 10.282230708999123
# Время выполнения с вложенными тасками (5 страниц): 9.705754711998452
# Время выполнения с вложенными тасками (30 страниц): 27.03962082599901
