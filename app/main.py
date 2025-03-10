import time
from datetime import datetime

from database.crud import mass_create_trade
from exceptions import RequestProcessingError, XLSExtractorError
from logging_config import logger
from parsers.parser import Parser
from parsers.scraper import download_file_as_bytes, get_page
from utils.file_utils import XLSExtractor

BASE_URL = "https://spimex.com"
START_PAGE = (
    "https://spimex.com/markets/oil_products/trades/results/?page=page-1"
)
CURRENT_YEAR = datetime.now().year
MIN_YEAR = 2023


def main():
    """Главный модуль"""
    current_page = START_PAGE
    count_entries = 0
    count_pages = 0
    logger.info("Начало парсинга страниц...")
    try:
        while current_page:
            try:
                logger.info(f"Обрабатывается страница: {current_page}")
                # Запрашиваем страницу
                content = get_page(current_page)
                parser = Parser(content, MIN_YEAR, CURRENT_YEAR)
            # Останавливаем цикл если загрузка страницы вызывает исключения
            except RequestProcessingError as e:
                logger.error(
                    f"Ошибка при загрузке страницы {current_page}: {e}"
                )
                break  # Останавливаем выполнение

            # Получаем ссылки и дату торгов
            # если список пуст заканчиваем цикл
            link_files = parser.extract_files()
            if not link_files:
                logger.info("Файлы не найдены, завершаем работу.")
                break

            # В цикле по списку загружаем файл и получаем из него данные
            logger.info(f"Скачивание файлов со страницы {current_page}")
            for file_url, bidding_date in link_files:
                try:
                    # Загружаем файл
                    file_stream = download_file_as_bytes(BASE_URL + file_url)
                    # Получаем данные из файла
                    xls_extractor = XLSExtractor(file_stream, bidding_date)
                    data = xls_extractor.get_data()
                    logger.info(
                        f"Начало загрузки данных торгов {bidding_date} в БД"
                    )
                    # Сохраняем данные в БД
                    mass_create_trade(data)
                    count_entries += 1
                    logger.info(
                        f"Торги {bidding_date} сохранены ({count_entries})"
                    )

                except XLSExtractorError as e:
                    logger.error(
                        f"Ошибка обработки файла {file_url}: {e}",
                        exc_info=True,
                    )
                    continue

                except RequestProcessingError as e:
                    logger.error(
                        f"Ошибка загрузки файла {file_url}: {e}", exc_info=True
                    )
                    continue

                except Exception as e:
                    logger.error(
                        f"Неизвестная ошибка обработки файла {file_url}: {e}",
                        exc_info=True,
                    )
                    continue

            # Получаем ссылку на следующую страницу
            next_page = parser.get_next_page()
            if not next_page:
                logger.info("Следующая страница не найдена")
                break
            current_page = BASE_URL + next_page
            count_pages += 1
            logger.info(f"Обработано {count_pages} страниц")
            time.sleep(2)

    except KeyboardInterrupt:
        logger.warning(
            "Программа прервана пользователем (Ctrl + C). Завершение работы..."
        )
    except Exception as e:
        logger.error(f"Программа прервана {e}", exc_info=True)
    finally:
        logger.info(
            f"Завершение работы. Всего обработано {count_pages} страниц. "
            f"В БД сохранено {count_entries} записей."
        )


if __name__ == "__main__":
    main()
