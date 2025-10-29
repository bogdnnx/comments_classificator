# utils.py
import asyncio
import hashlib
from concurrent.futures import ThreadPoolExecutor
from typing import List
from aiohttp import ClientSession
from config import VK_ACCESS_TOKEN, CACHE_TTL
from classifier import SentimentClassifierStub as SentimentClassifier


# Глобальный экземпляр классификатора и executor (при условии, что classifier потокобезопасен)
_classifier = SentimentClassifier()
_executor = ThreadPoolExecutor(max_workers=4)


def make_cache_key(query: str, count: int) -> str:
    """Создаёт уникальный ключ кэша для поискового запроса."""
    key_str = f"search:{query.strip().lower()}:{count}"
    return hashlib.md5(key_str.encode()).hexdigest()


async def classify_texts_async(texts: List[str]):
    """Асинхронно классифицирует список текстов с использованием пула потоков."""
    loop = asyncio.get_event_loop()
    # Предполагается, что у классификатора есть метод predict_in_batches или аналогичный
    result = await loop.run_in_executor(_executor, _classifier.predict_in_batches, texts)
    return result


# Глобальное состояние для лимитера VK API (на один процесс)
_request_timestamps = []


async def vk_request(method: str, params: dict) -> dict:
    """
    Выполняет асинхронный запрос к VK API с соблюдением лимита: не более 3 запросов в секунду.
    """
    global _request_timestamps

    now = asyncio.get_event_loop().time()

    # Очищаем времена старше 1 секунды
    _request_timestamps = [t for t in _request_timestamps if now - t < 1.0]

    # Если достигнут лимит — ждём до конца текущей секунды
    if len(_request_timestamps) >= 3:
        sleep_time = 1.0 - (now - _request_timestamps[0])
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)
        _request_timestamps = []  # сбрасываем окно

    _request_timestamps.append(now)

    # Подготавливаем параметры
    params = {
        **params,
        "access_token": VK_ACCESS_TOKEN,
        "v": "5.199"  # актуальная версия API
    }

    # Исправлен URL: убраны лишние пробелы
    url = f"https://api.vk.com/method/{method}"

    async with ClientSession() as session:
        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    print(f"⚠️  VK API HTTP {resp.status}")
                    return {}
                data = await resp.json()
                if "error" in data:
                    error = data["error"]
                    print(f"⚠️  VK API Error {error.get('error_code')}: {error.get('error_msg')}")
                    return {}
                return data.get("response", {})
        except Exception as e:
            print(f"❌ Ошибка при запросе к VK API ({method}): {e}")
            return {}