import io

import requests
from exceptions import RequestProcessingError
from logging_config import logger
from requests.exceptions import ConnectionError, RequestException, Timeout


def get_request(url: str) -> requests.Response:
    """Выполняет GET-запрос по указанному URL и возвращает объект Response"""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response
    except (ConnectionError, Timeout) as e:
        raise RequestProcessingError(
            f"Ошибка при подключении или тайм-аут для страницы {url}: {e}"
        ) from e
    except RequestException as e:
        raise RequestProcessingError(
            f"Ошибка запроса при получении страницы {url}: {e}"
        ) from e
    except Exception as e:
        raise RequestProcessingError(
            f"Неизвестная ошибка при получении страницы {url}: {e}"
        ) from e


def get_page(url: str) -> str:
    """Получает HTML-код страницы по указанному URL"""
    try:
        response = get_request(url)
        return response.text
    except RequestProcessingError as e:
        logger.error(str(e))
        raise RequestProcessingError(e) from e


def download_file_as_bytes(url: str) -> io.BytesIO:
    """Скачивает файл по ссылке и возвращает его в виде потока байтов"""
    try:
        response = get_request(url)
        return io.BytesIO(response.content)
    except RequestProcessingError as e:
        logger.error(str(e))
        raise RequestProcessingError(e) from e
