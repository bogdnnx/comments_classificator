# utils.py
import asyncio
import hashlib
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import List
import redis.asyncio as redis
from aiohttp import ClientSession
from config import VK_ACCESS_TOKEN, REDIS_URL, CACHE_TTL # Предполагается существование файла config.py
from classifier import SentimentClassifierStub as SentimentClassifier # Предполагается существование файла classifier.py

# --- Глобальные переменные/объекты ---
classifier = SentimentClassifier()
executor = ThreadPoolExecutor(max_workers=4)
r = redis.from_url(REDIS_URL, decode_responses=True)

# --- Вспомогательные функции ---
def make_cache_key(query: str, count: int) -> str:
    key_str = f"search:{query.strip().lower()}:{count}"
    return hashlib.md5(key_str.encode()).hexdigest()

async def classify_texts_async(texts: List[str]):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, classifier.predict_in_batches, texts)

request_timestamps = []

async def vk_request(method: str, params: dict) -> dict:
    global request_timestamps
    now = asyncio.get_event_loop().time()
    # Очистка старых времён (старше 1 сек)
    request_timestamps = [t for t in request_timestamps if now - t < 1.0]
    # Если уже 3 запроса за последнюю секунду ждём
    if len(request_timestamps) >= 3:
        sleep_time = 1.0 - (now - request_timestamps[0])
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)
        request_timestamps = []  # сбрасываем окно
    request_timestamps.append(now)

    params.update({
        "access_token": VK_ACCESS_TOKEN,
        "v": "5.131"
    })
    async with ClientSession() as session:
        async with session.get(f"https://api.vk.com/method/{method}", params=params) as resp:
            data = await resp.json()
            if "error" in data:
                print(f"VK API Error: {data['error']}")
                return {}
            return data.get("response", {})

# Другие вспомогательные функции можно добавить сюда.