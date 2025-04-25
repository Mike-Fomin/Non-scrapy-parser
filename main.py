import asyncio
import json
import logging
import time
import traceback
from itertools import cycle
from typing import Any

import aiohttp
import requests
from requests import Response
from requests.exceptions import RequestException
from tqdm.asyncio import tqdm


# Логирование
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    style='{',
    format='{filename}:{lineno} #{levelname:8} [{asctime}] - {name} - {message}'
)

# Читаем ссылки из файла "input_urls.txt"
with open('input_urls.txt', 'r', encoding='utf-8') as inp_file:
    INPUT_URLS: list[str] = list(map(str.strip, inp_file.readlines())) # Убираем символ переноса строки

# Читаем прокси из файла "proxy_http_ip.txt"
with open('proxy_http_ip.txt', 'r', encoding='utf-8') as proxy_file:
    PROXIES: list[str] = list(map(str.strip, proxy_file.readlines())) # Убираем символ переноса строки

# UUID городов в параметрах API
CITIES_ID: dict = {
    "Москва": "396df2b5-7b2b-11eb-80cd-00155d03900",
    "Краснодар": "4a70f9e0-46ae-11e7-83ff-00155d026416",
    "Ростов-на-Дону": "878a9eb4-46b2-11e7-83ff-00155d026416",
    "Сочи": "985b3eea-46b4-11e7-83ff-00155d026416"
}


async def get_item_info(
        item_slug: str,
        proxy: str,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        city_id: str
) -> dict[str, Any] | None:
    """Асинхронно получает информацию о товаре с сайта по его slug.

    Args:
        item_slug (str): Уникальный идентификатор товара (slug).
        proxy (str): Адрес прокси-сервера в формате "host:port".
        session (aiohttp.ClientSession): Сессия для выполнения HTTP-запросов.
        city_id (str): UUID города для запроса.

    Returns:
        dict[str, Any] | None: Словарь с данными о товаре или None в случае ошибки.

    Raises:
        aiohttp.ClientError: Если возникла ошибка соединения или запроса.
    """

    headers: dict[str, str] = {
        'accept': '*/*',
        'accept-language': 'ru,ru-RU;q=0.9,en-US;q=0.8,en;q=0.7,th;q=0.6',
        'priority': 'u=1, i',
        'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
    }
    params: dict[str, str] = {
        "city_uuid": city_id
    }

    # Подключаем семафор
    async with semaphore:

        # Управляемый цикл переподключений на случай проблем с соединением
        tries: int = 0 # Колчичество попыток соединения (настраиваемый параметр)
        sleep_timer: int = 3 # Задержка между повторными запросами в секундах в случае ошибки (настраиваемый параметр)
        request_delay: int = 0.2 # Задержка между запросами в секундах (настраиваемый параметр)
        error_traceback: str = '' # Трассировка ошибки
        while True:
            if tries >= 3:
                logger.error(error_traceback)
                return
            try:
                await asyncio.sleep(request_delay) # Добавляем задержку перед запросом, чтобы ограничить частоту запросов
                item_response: aiohttp.ClientResponse = await session.get(
                    url=f"https://alkoteka.com/web-api/v1/product/{item_slug}",
                    params=params,
                    headers=headers,
                    proxy=f"http://{proxy}",
                    proxy_auth=aiohttp.BasicAuth(login="irusp1050411", password="SGvSzxMcJb")
                )
            except aiohttp.ClientError:
                logger.info(f"Product {item_slug} connection error!")
                error_traceback: str = traceback.format_exc()
                await asyncio.sleep(sleep_timer)
                sleep_timer += 3
                tries += 1
            else:
                # logger.info(f"{item_slug} status code = {item_response.status}")
                if item_response.status == 200:
                    resp_timestamp: int = int(time.time())  # Получаем текущее время в формате timestamp
                    break
                else:
                    logger.warning(f"{item_slug} status code = {item_response.status}")
                    await asyncio.sleep(sleep_timer)
                    sleep_timer += 3
                    tries += 1

    # Преобразуем ответ к json
    item_json: dict[str, Any] = await item_response.json()
    item: dict[str, Any] = item_json['results']

    # Получаем "объем" из фильтров товара для названия
    volume: str = ""
    for filter_label in item.get('filter_labels', []):
        filter_label: dict[str, Any]
        if filter_label.get('filter', '') == "obem":
            volume: str = filter_label.get('title')
            break

    # Получаем характеристики товара
    brand: str = ""
    all_chars: dict[str, str] = {}
    for char in item.get('description_blocks', []):
        char: dict[str, Any]

        # Получаем бренд товара
        if char.get('code', '') == "brend":
            brand: str = char['values'][0].get('name', '')
        # Сохраняем характеристики в зависимости от вида
        match char.get('type', ''):
            case "range":
                all_chars[char['title']] = f"{char.get('max', '')}{char.get('unit', '')}"
            case "select":
                all_chars[char['title']] = char['values'][0].get('name', '')
            case "flag":
                all_chars[char['title']] = char['placeholder']

    result: dict[str, Any] = {
        "timestamp": resp_timestamp, # Дата и время сбора товара в формате timestamp
        "RPC": item.get('vendor_code'), # Артикул товара
        "url": f"https://alkoteka.com/product/{item['category']['slug']}/{item_slug}", # Ссылка на товар
        "title": f"{item.get('name')} {volume}" if volume else f"{item.get('name')}", # Название товара
        "marketing_tags": [fl.get('title', '') for fl in item.get('filter_labels', [])], # Список маркетинговых тэгов
        "brand": brand, # Бренд товара
        "section": [item['category']['parent'].get('name', ''), item['category'].get('name', '')], # Иерархия разделов
        "price_data": {
            "current": float(item['price']) if item.get('price') else float(item.get('prev_price', 0)), # Цена со скидкой
            "original": float(item.get('prev_price', 0)), # Оригинальная цена
            "sale_tag": f"Скидка {100 - round(100 * (item.get('price', 0) / item.get('prev_price', 0)))}%" # Процент скидки
        },
        "stock": {
            "in_stock": item.get('available', False), # Товар в наличии
            "count": item.get('quantity_total', 0) # Количество оставшегося товара
        },
        "assets": {
            "main_image": item.get('image_url', ''),  # Ссылка на основное изображение товара
            "set_images": [],
            "view360": [],
            "video": []
        },
        "metadata": {
            "__description": "".join([
                title.get('content', '').replace('<br>\n', '. ')
                for title in item.get('text_blocks', []) if title.get('title', '') == 'Описание'
            ]) # Описание товара
        },
        "variants": 1
    }

    # Добавляем в результат характеристики товара
    result['metadata'].update(all_chars)

    return result


async def main():
    """Асинхронная точка входа для парсинга товаров с сайта alkoteka.com.

    Обрабатывает список категорий из файла input_urls.txt, получает данные о товарах
    через API сайта для указанного города (Краснодар) и сохраняет результаты в файл result.json.
    Использует асинхронные запросы через aiohttp с прокси из файла proxy_http_ip.txt.

    Args:
        None

    Returns:
        None

    Raises:
        requests.exceptions.RequestException: Если возникла ошибка при синхронном запросе к API категорий.
        aiohttp.ClientError: Если возникла ошибка при асинхронных запросах к API товаров.

    Side Effects:
        Создаёт файл result.json с данными о товарах в формате JSON.
        Логирует информацию и ошибки в консоль.
    """

    parse_result: list[dict] = []

    semaphore = asyncio.Semaphore(10) # Семафор для контроля нагрузки

    # UUID требуемого города
    city_id: str = CITIES_ID["Краснодар"]

    try:
        for category_url in INPUT_URLS:
            # Получаем название категории из ссылки
            category_name: str = category_url.split('/')[-1]

            headers: dict[str, str] = {
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            }
            params: dict[str, str] = {
                'city_uuid': city_id, # Код города в API
                'page': '1',
                'per_page': '10000', # Количество элементов на странице (10_000 - чтобы получить все товары в категории)
                'root_category_slug': category_name
            }

            try:
                response: Response = requests.get(
                    url='https://alkoteka.com/web-api/v1/product',
                    params=params,
                    headers=headers
                )
                logger.info(f"Status code = {response.status_code}")
            except RequestException:
                logger.info(f"Connection problem!")
                logger.error(traceback.format_exc())
                continue

            # Преобразуем ответ к json
            json_resp: dict[str, Any] = response.json()

            total_goods: int = len(json_resp['results']) # Общее количество товаров в категории
            if not total_goods:
                logger.info(f"Товаров в данной категории нет")
                continue

            logger.info(f"Количество товаров в категории {json_resp['results'][0]['category']['parent']['name']}: "
                        f"{total_goods}")

            # Получаем список "slug" товаров
            slugs: list[str] = list(map(lambda x: x['slug'], json_resp['results']))

            # Создаем асинхронные запросы к API сайта
            async with aiohttp.ClientSession() as session:
                tasks: list = []
                for slug, proxy in zip(slugs, cycle(PROXIES)):
                    task = asyncio.ensure_future(get_item_info(slug, proxy, session, semaphore,  city_id))
                    tasks.append(task)

                # Получаем результат по готовности
                for future in tqdm.as_completed(tasks, total=len(tasks)):
                    res: dict | None = await future
                    if res:
                        parse_result.append(res)
    except Exception:
        logger.info(f"Ошибка в main")
        logger.error(traceback.format_exc())
    finally:
        # Запись результата в файл "result.json"
        logger.info(f"Всего товаров = {len(parse_result)}")
        with open('result.json', 'w', encoding='utf-8') as res_file:
            json.dump(parse_result, res_file, indent=4, ensure_ascii=False)


if __name__ == '__main__':
    asyncio.run(main())