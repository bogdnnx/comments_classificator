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

# Подключение к Redis
r = redis.from_url(REDIS_URL, decode_responses=True)

@app.on_event('startup')
async def startup_event():
    await init_db()

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/search/")
async def search_posts(
    request: Request,
    tag: str = Form(...),
    count: int = Form(10),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: AsyncSession = Depends(get_db)
):
    task_id = str(uuid.uuid4())
    cache_key = await make_cache_key(task_id)

    initial_status = {
        "status": "processing",
        "progress": {"current": 0, "total": count},
        "query_text": tag,
        "count": count
    }
    await r.setex(cache_key, CACHE_TTL, initial_status)

    background_tasks.add_task(process_comments_async, task_id, tag, count, db)

    return RedirectResponse(url=f"/status/{task_id}", status_code=303)

@app.get("/status/{task_id}")
async def get_status(task_id: str):
    cache_key = await make_cache_key(task_id)
    status_data = await r.get(cache_key)

    if not status_data:
        raise HTTPException(status_code=404, detail="Task not found or expired")

    return status_data

@app.get("/results/{task_id}")
async def get_results(task_id: str, db: AsyncSession = Depends(get_db)):
    cache_key = await make_cache_key(task_id)
    status_data = await r.get(cache_key)

    if not status_data:
        raise HTTPException(status_code=404, detail="Results not found or expired")

    if status_data.get("status") != "completed":
        return status_data

    query_result = await db.execute(
        select(SearchQuery).where(SearchQuery.task_id == task_id)
    )
    search_query = query_result.scalar_one_or_none()

    if not search_query:
        raise HTTPException(status_code=404, detail="Results for this task ID not found in database")

    posts_result = await db.execute(
        select(Post).where(Post.search_query_id == search_query.id)
    )
    posts = posts_result.scalars().all()

    results = []
    for post in posts:
        comments_result = await db.execute(
            select(Comment).where(Comment.post_id == post.id)
        )
        comments = comments_result.scalars().all()

        post_data = {
            "post_id": post.vk_post_id,
            "owner_id": post.owner_id,
            "text": post.text,
            "url": post.url,
            "comments": [
                {
                    "comment_id": c.vk_comment_id,
                    "text": c.text,
                    "sentiment": c.sentiment,
                    "confidence": c.sentiment_confidence
                }
                for c in comments
            ]
        }
        results.append(post_data)

    return {"task_id": task_id, "query": search_query.query_text, "results": results}

# Запуск приложения
