import redis.asyncio as redis

import asyncio
import hashlib
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import List
from fastapi import FastAPI, Request, Form, Depends, BackgroundTasks
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse
from aiohttp import ClientSession
from datetime import datetime, timedelta

# SQLAlchemy импорты (добавлены!)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

# Конфиг и компоненты
from config import VK_ACCESS_TOKEN, REDIS_URL, CACHE_TTL
from classifier import SentimentClassifierStub as SentimentClassifier
from database import get_db, init_db
from models import SearchQuery, Post, Comment

app = FastAPI()
templates = Jinja2Templates(directory="templates")
#app.mount("/static", StaticFiles(directory="static"), name="static")

# Инициализация
classifier = SentimentClassifier()
executor = ThreadPoolExecutor(max_workers=4)
#r = redis.from_url(REDIS_URL, decode_responses=True)
# RedisStub с поддержкой состояния задач
class RedisStub:
    def __init__(self):
        self._data = {}
        self._hashes = {}

    async def get(self, key):
        return self._data.get(key)

    async def setex(self, key, ttl, value):
        self._data[key] = value

    async def hset(self, key, *args, mapping=None, **kwargs):
        if key not in self._hashes:
            self._hashes[key] = {}

        # Поддержка: hset(key, field, value)
        if len(args) == 2:
            field, value = args
            self._hashes[key][field] = value
        # Поддержка: hset(key, mapping={...})
        elif mapping is not None:
            self._hashes[key].update(mapping)
        # Поддержка: hset(key, field1=value1, field2=value2)
        elif kwargs:
            self._hashes[key].update(kwargs)

    async def hgetall(self, key):
        return self._hashes.get(key, {})

    async def expire(self, key, ttl):
        pass

r = RedisStub()

r = RedisStub()

@app.on_event("startup")
async def on_startup():
    await init_db()

# Хэширование ключа кэша
def make_cache_key(query: str, count: int) -> str:
    key_str = f"search:{query.strip().lower()}:{count}"
    return hashlib.md5(key_str.encode()).hexdigest()

# Асинхронный вызов классификатора
async def classify_texts_async(texts: List[str]):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, classifier.predict_in_batches, texts)

# VK API запрос
async def vk_request(method: str, params: dict) -> dict:
    params.update({
        "access_token": VK_ACCESS_TOKEN,
        "v": "5.199"
    })
    async with ClientSession() as session:
        async with session.get(f"https://api.vk.com/method/{method}", params=params) as resp:
            data = await resp.json()
            return data.get("response", {})

# Фоновая задача обработки
async def process_comments_async(task_id: str, query: str, count: int, cache_key: str, db_session):
    try:
        # 1. Поиск постов
        posts_data = await vk_request("newsfeed.search", {"q": query, "count": min(count, 200), "extended": 1})
        posts = posts_data.get("items", [])

        # 2. Создаём запись поиска в БД
        expires_at = datetime.utcnow() + timedelta(seconds=CACHE_TTL)
        search_query = SearchQuery(
            query_text=query,
            count=count,
            task_id=task_id,
            expires_at=expires_at
        )
        db_session.add(search_query)
        await db_session.flush()

        all_comments = []
        all_texts = []

        # 3. Для каждого поста — получаем комментарии
        for post in posts:
            owner_id = post["owner_id"]
            post_id = post["id"]
            comments_data = await vk_request("wall.getComments", {
                "owner_id": owner_id,
                "post_id": post_id,
                "count": 100
            })
            comments = comments_data.get("items", [])
            for comment in comments:
                text = comment.get("text", "").strip()
                if text:
                    all_comments.append({
                        "comment": comment,
                        "post": post,
                        "owner_id": owner_id,
                        "post_id": post_id
                    })
                    all_texts.append(text)

        # 4. Классификация
        if all_texts:
            labels, probs = await classify_texts_async(all_texts)

            # 5. Сохраняем посты и комментарии
            post_cache = {}  # кэшируем id постов в БД, чтобы не дублировать

            for i, item in enumerate(all_comments):
                if i >= len(labels):
                    break

                post = item["post"]
                owner_id = item["owner_id"]
                post_id = item["post_id"]
                comment = item["comment"]

                # Проверяем, сохранён ли уже этот пост
                if (owner_id, post_id) not in post_cache:
                    db_post = Post(
                        vk_post_id=post_id,
                        owner_id=owner_id,
                        text=post.get("text", "")[:5000],
                        date=post.get("date"),
                        url=f"https://vk.com/wall{owner_id}_{post_id}",
                        search_query_id=search_query.id
                    )
                    db_session.add(db_post)
                    await db_session.flush()
                    post_cache[(owner_id, post_id)] = db_post.id
                else:
                    db_post_id = post_cache[(owner_id, post_id)]

                # Сохраняем комментарий
                db_comment = Comment(
                    vk_comment_id=comment["id"],
                    post_id=post_cache[(owner_id, post_id)],
                    from_id=comment.get("from_id"),
                    text=comment["text"][:2000],
                    sentiment="positive" if labels[i] == 1 else "negative",
                    sentiment_confidence=float(probs[i]),
                    date=comment.get("date")
                )
                db_session.add(db_comment)

        await db_session.commit()

        # 6. Обновляем кэш
        await r.setex(cache_key, CACHE_TTL, task_id)
        await r.hset(f"task:{task_id}", mapping={"status": "error"})

    except Exception as e:
        print(f"Ошибка в задаче {task_id}: {e}")
        await r.hset(f"task:{task_id}", mapping={"status": "error"})

# Роуты
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/search", response_class=HTMLResponse)
async def search_posts(
    request: Request,
    background_tasks: BackgroundTasks,
    query: str = Form(...),
    count: int = Form(10),
    db: AsyncSession = Depends(get_db)
):
    cache_key = make_cache_key(query, count)
    cached_task_id = await r.get(cache_key)

    if cached_task_id:
        return templates.TemplateResponse("results_redirect.html", {
            "request": request,
            "task_id": cached_task_id
        })

    task_id = str(uuid.uuid4())
    await r.setex(cache_key, CACHE_TTL, task_id)

    background_tasks.add_task(process_comments_async, task_id, query, count, cache_key, db)

    return templates.TemplateResponse("results_loading.html", {
        "request": request,
        "task_id": task_id,
        "query": query
    })

@app.get("/status/{task_id}")
async def get_status(task_id: str):
    status_data = await r.hgetall(f"task:{task_id}")
    if not status_data:
        return {"status": "not_found"}
    return {"status": status_data.get("status", "processing")}

@app.get("/results/{task_id}", response_class=HTMLResponse)
async def show_results(request: Request, task_id: str, db: AsyncSession = Depends(get_db)):
    # Получаем поиск
    result = await db.execute(
        select(SearchQuery).where(SearchQuery.task_id == task_id)
    )
    search_query = result.scalar_one_or_none()

    if not search_query:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": "Результаты удалены или не найдены. Повторите поиск."
        })

    # Получаем посты
    posts_result = await db.execute(
        select(Post).where(Post.search_query_id == search_query.id)
    )
    posts = posts_result.scalars().all()

    posts_with_comments = []
    total_positive = 0
    total_negative = 0

    for post in posts:
        comments_result = await db.execute(
            select(Comment).where(Comment.post_id == post.id)
        )
        comments = comments_result.scalars().all()
        for c in comments:
            if c.sentiment == "positive":
                total_positive += 1
            else:
                total_negative += 1
        posts_with_comments.append({
            "post": post,
            "comments": comments,
            "post_link": post.url
        })

    return templates.TemplateResponse("results.html", {
        "request": request,
        "query": search_query.query_text,
        "posts": posts_with_comments,
        "summary": {
            "positive": total_positive,
            "negative": total_negative
        }
    })