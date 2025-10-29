# utils.py
import asyncio
import logging
from typing import List, Dict, Any
import redis.asyncio as redis
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from models import Comment, Post, SearchQuery
from classifier import SentimentClassifierStub
from config import REDIS_URL, CACHE_TTL

# Инициализируем классификатор как глобальную переменную
classifier = SentimentClassifierStub()

async def make_cache_key(task_id: str) -> str:
    """Создает ключ для кэша Redis по ID задачи."""
    # Просто возвращаем строку с префиксом, как часто делают
    return f"task_status:{task_id}"

async def classify_comments_batch_async(texts: List[str]) -> tuple[List[str], List[float]]:
    """
    Асинхронно классифицирует батч комментариев.
    В текущей реализации predict_in_batches может быть синхронным,
    но мы оборачиваем его в asyncio.to_thread, если это тяжелая операция.
    """
    # Если predict_in_batches может блокировать, используем to_thread
    # labels, confidences = await asyncio.to_thread(classifier.predict_in_batches, texts)
    # Если нет, можно вызвать напрямую
    labels, confidences = classifier.predict_in_batches(texts)
    return labels, confidences

async def update_task_status(task_id: str, status: str, progress: Dict[str, Any] = None):
    """Обновляет статус задачи в Redis."""
    cache_key = await make_cache_key(task_id)
    status_data = {
        "status": status,
        "progress": progress or {}
    }
    # Используем TTL из конфига
    await r.setex(cache_key, CACHE_TTL, status_data)

# Подключение к Redis для использования в утилитах
r = redis.from_url(REDIS_URL, decode_responses=True)