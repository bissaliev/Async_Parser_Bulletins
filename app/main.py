import asyncio
import time
from datetime import date, datetime

import aiohttp
from database.crud import mass_create_trade
from logging_config import logger
from parsers.parser import Parser
from parsers.scraper import fetch_file, fetch_page
from utils.file_utils import XLSExtractor

BASE_URL = "https://spimex.com"
PAGE_URL = BASE_URL + "/markets/oil_products/trades/results/"
CURRENT_YEAR = datetime.now().year
MIN_YEAR = 2023
FIRST_PAGE = 1
LAST_PAGE = 55
MAX_CONCURRENT_REQUESTS = 15  # Максимальное число одновременных запросов
MAX_DB_CONCURRENT = 10  # Ограничение для операций с базой данных


async def download_data(
    session: aiohttp.ClientSession, url: str, bidding_date: date, semaphore: asyncio.Semaphore
) -> None:
    """Скачивает файл, обрабатывает его и сохраняет данные в БД"""

    content = await fetch_file(session, url)

    if content is None:
        logger.error(f"Не удалось скачать файл с торгами за {bidding_date} период")
    else:
        # Создаем класс извлекающий нужные данные из файла xls
        xls_extractor = XLSExtractor(content, bidding_date)
        data = xls_extractor.get_data()

        # Сохраняем данные в БД
        async with semaphore:
            await mass_create_trade(data)


async def process_page(session: aiohttp.ClientSession, page: int, semaphore: asyncio.Semaphore):
    """Обрабатывает одну страницу: парсит ссылки и загружает файлы"""
    page_html = await fetch_page(session, PAGE_URL, params={"page": f"page-{page}"})
    if page_html is None:
        logger.warning(f"Пропускаем страницу {page}, так как HTML не был загружен")
        return
    parser = Parser(page_html, MIN_YEAR, CURRENT_YEAR)
    file_links: list[tuple[str, date]] = parser.extract_file_links()
    tasks = []
    for link, bidding_date in file_links:
        tasks.append(asyncio.create_task(download_data(session, BASE_URL + link, bidding_date, semaphore)))
    await asyncio.gather(*tasks)
    logger.info(f"Страница {page} загружена")


async def main():
    """Главный модуль"""
    # В цикле проходимся по страницам со ссылка на файлы
    tasks = []
    semaphore_db = asyncio.Semaphore(MAX_DB_CONCURRENT)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        for page in range(FIRST_PAGE, LAST_PAGE + 1):
            tasks.append(asyncio.create_task(process_page(session, page, semaphore_db)))
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
