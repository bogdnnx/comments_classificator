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


async def vk_execute(code: str) -> any:
    """
    Выполняет execute-запрос к VK API с тем же rate limiting'ом, что и vk_request.
    Возвращает значение поля 'response' из ответа VK (обычно — список).
    """
    global _request_timestamps

    now = asyncio.get_event_loop().time()
    _request_timestamps = [t for t in _request_timestamps if now - t < 1.0]

    if len(_request_timestamps) >= 3:
        sleep_time = 1.0 - (now - _request_timestamps[0])
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)
        now = asyncio.get_event_loop().time()
        _request_timestamps = []

    _request_timestamps.append(now)

    # Параметры для execute
    full_params = {
        "code": code,
        "access_token": VK_ACCESS_TOKEN,
        "v": "5.199"
    }

    url = "https://api.vk.com/method/execute"

    async with ClientSession() as session:
        try:
            async with session.get(url, params=full_params) as resp:
                if resp.status != 200:
                    print(f"⚠️  VK API HTTP {resp.status} for method execute")
                    return []
                data = await resp.json()
                if "error" in data:
                    error = data["error"]
                    print(f"⚠️  VK API Error {error.get('error_code')}: {error.get('error_msg')} (method: execute)")
                    # Логируем внутренние ошибки выполнения, если есть
                    if "execute_errors" in data:
                        print(f"   Внутренние ошибки execute: {data['execute_errors']}")
                    return []
                return data.get("response", [])
        except Exception as e:
            print(f"❌ Ошибка при execute-запросе: {e}")
            return []


async def fetch_comments_via_execute(posts: List[tuple[int, int]]) -> List[dict[str, any]]:
    if not posts:
        return []

    all_comments = []
    BATCH_SIZE = 25

    for i in range(0, len(posts), BATCH_SIZE):
        batch = posts[i:i + BATCH_SIZE]
        calls = [
            f'API.wall.getComments({{"owner_id":{oid},"post_id":{pid},"count":100}})'
            for oid, pid in batch
        ]
        code = f"return [{','.join(calls)}];"

        results = await vk_execute(code)

        if not isinstance(results, list):
            print(f"⚠️ vk_execute вернул не список: {type(results)}")
            continue

        if len(results) != len(batch):
            print(f"⚠️ Несоответствие длины: batch={len(batch)}, results={len(results)}")
            results = results[:len(batch)]

        for (owner_id, post_id), res in zip(batch, results):
            if not isinstance(res, dict):
                continue
            if "error" in res:
                err = res["error"]
                print(f"⚠️ wall.getComments ошибка для {owner_id}_{post_id}: {err.get('error_code')} – {err.get('error_msg')}")
                continue
            for comment in res.get("items", []):
                if comment.get("text", "").strip():
                    all_comments.append({
                        "owner_id": owner_id,
                        "post_id": post_id,
                        "comment": comment
                    })

    return all_comments

