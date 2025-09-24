# main.py
import asyncio
import hashlib
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import List
from fastapi import FastAPI, Request, Form, Depends, BackgroundTasks
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse
import redis.asyncio as redis
from aiohttp import ClientSession
from datetime import datetime, timedelta
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config import VK_ACCESS_TOKEN, REDIS_URL, CACHE_TTL
from classifier import SentimentClassifierStub as SentimentClassifier
from database import get_db, init_db, AsyncSessionLocal
from models import SearchQuery, Post, Comment

app = FastAPI()
templates = Jinja2Templates(directory="templates")
#app.mount("/static", StaticFiles(directory="static"), name="static")

classifier = SentimentClassifier()
executor = ThreadPoolExecutor(max_workers=4)
r = redis.from_url(REDIS_URL, decode_responses=True)

@app.on_event("startup")
async def on_startup():
    await init_db()
    print("✅ Таблицы в БД созданы (если их ещё не было)")

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

    # Если уже 3 запроса за последнюю секунду жджём
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

async def process_comments_async(task_id: str, query: str, count: int, cache_key: str):
    try:
        print(f"🚀 Начинаем обработку задачи {task_id} для запроса: {query}")
        # Обозначаем старт задачи, если ещё не отмечено
        await r.hset(f"task:{task_id}", mapping={"status": "processing"})
        async with AsyncSessionLocal() as db_session:
            posts_data = await vk_request("newsfeed.search", {"q": query, "count": min(count, 200), "extended": 1})
            if not posts_data:
                print("   ❌ Ответ от newsfeed.search пустой — проверь URL и токен")
                await r.hset(f"task:{task_id}", mapping={"status": "error", "error": "empty_response"})
                return
            posts = posts_data.get("items", [])
            print(f"   Найдено постов: {len(posts)}")

            expires_at = datetime.utcnow() + timedelta(seconds=CACHE_TTL)
            search_query = SearchQuery(
                query_text=query,
                count=count,
                task_id=task_id,
                expires_at=expires_at
            )
            db_session.add(search_query)
            await db_session.flush()

            if not posts:
                print("   ❌ Нет постов — завершаем задачу (пустые результаты)")
                await db_session.commit()
                await r.setex(cache_key, CACHE_TTL, task_id)
                await r.hset(f"task:{task_id}", mapping={"status": "done", "message": "no_posts"})
                return

            all_comments = []
            all_texts = []
            post_cache = {}

            # Сохраняем все посты сразу даже если комментариев нет
            for post in posts:
                owner_id = post["owner_id"]
                post_id = post["id"]
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

                # Загружаем комментарии к посту
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
                            "owner_id": owner_id,
                            "post_id": post_id
                        })
                        all_texts.append(text)

            if all_texts:
                labels, confidences = await classify_texts_async(all_texts)

                for i, item in enumerate(all_comments):
                    if i >= len(labels):
                        break

                    owner_id = item["owner_id"]
                    post_id = item["post_id"]
                    comment = item["comment"]


                    db_comment = Comment(
                        vk_comment_id=comment["id"],
                        post_id=post_cache[(owner_id, post_id)],
                        from_id=comment.get("from_id"),
                        text=comment["text"][:2000],
                        sentiment=labels[i],
                        sentiment_confidence=float(confidences[i]),
                        date=comment.get("date")
                    )
                    db_session.add(db_comment)

            if all_texts:
                print(f"   Сохранено комментариев: {len(all_texts)}")
            else:
                print("   ❌ Нет комментариев для сохранения")

            await db_session.commit()
            await r.setex(cache_key, CACHE_TTL, task_id)
            await r.hset(f"task:{task_id}", mapping={"status": "done"})
            print(f"✅ Задача {task_id} успешно завершена")

    except Exception as e:
        print(f"❌ Ошибка в задаче {task_id}: {e}")
        await r.hset(f"task:{task_id}", mapping={"status": "error", "error": str(e)})

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
        status_data = await r.hgetall(f"task:{cached_task_id}")
        status = status_data.get("status") if status_data else None
        if status == "done":
            return RedirectResponse(url=f"/results/{cached_task_id}", status_code=303)
        else:
            return templates.TemplateResponse("results_loading.html", {
                "request": request,
                "task_id": cached_task_id,
                "query": query
            })

    task_id = str(uuid.uuid4())
    # Отмечаем задачу как запущенную и сохраняем соответствие кэша
    await r.hset(f"task:{task_id}", mapping={"status": "processing"})
    await r.setex(cache_key, CACHE_TTL, task_id)
    background_tasks.add_task(process_comments_async, task_id, query, count, cache_key)

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
    result = await db.execute(select(SearchQuery).where(SearchQuery.task_id == task_id))
    search_query = result.scalar_one_or_none()

    if not search_query:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": "Результаты удалены или не найдены. Повторите поиск."
        })

    posts_result = await db.execute(select(Post).where(Post.search_query_id == search_query.id))
    posts = posts_result.scalars().all()

    post_ids = [post.id for post in posts]
    comments_result = await db.execute(select(Comment).where(Comment.post_id.in_(post_ids)))
    all_comments = comments_result.scalars().all()

    total_positive = sum(1 for c in all_comments if c.sentiment == "positive")
    total_negative = sum(1 for c in all_comments if c.sentiment == "negative")

    comments_by_post = {}
    for comment in all_comments:
        comments_by_post.setdefault(comment.post_id, []).append(comment)

    return templates.TemplateResponse("results.html", {
        "request": request,
        "query": search_query.query_text,
        "posts": posts,
        "comments_by_post": comments_by_post,
        "all_comments": all_comments,
        "summary": {
            "positive": total_positive,
            "negative": total_negative,
            "total": len(all_comments)
        }
    })