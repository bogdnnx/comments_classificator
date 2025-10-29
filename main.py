# main_app.py
import asyncio
from fastapi import FastAPI, Request, Form, Depends, BackgroundTasks
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession
from database import get_db, init_db # Предполагается существование файла database.py
from search_logic import process_comments_async # Импортируем основную функцию поиска
from utils import make_cache_key, r # Импортируем утилиты и Redis

app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.on_event("startup")
async def on_startup():
    await init_db()
    print("✅ Таблицы в БД созданы (если их ещё не было)")

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
    from utils import CACHE_TTL # Импорт внутри функции, если не глобальный

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

    task_id = asyncio.get_event_loop().create_future() # Используем Future для генерации ID в асинхронном контексте
    import uuid
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
    from sqlalchemy import select # Импорт внутри функции
    from models import SearchQuery, Post, Comment # Предполагается существование файла models.py

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

