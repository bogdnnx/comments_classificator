# main.py
import asyncio
import hashlib
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any
from fastapi import FastAPI, Request, Form, Depends, BackgroundTasks
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse
from datetime import datetime as dt
import redis.asyncio as redis
from aiohttp import ClientSession
from datetime import datetime, timedelta
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config import VK_ACCESS_TOKEN, REDIS_URL, CACHE_TTL
from classifier import SentimentClassifierStub as SentimentClassifier
from database import get_db, init_db, AsyncSessionLocal
from models import SearchQuery, Post, Comment, SearchQueryDay, DayPost

app = FastAPI()
templates = Jinja2Templates(directory="templates")


# Добавляем фильтр для форматирования даты
def timestamp_to_date(timestamp):
    if timestamp:
        return dt.fromtimestamp(timestamp).strftime("%d.%m.%y")
    return ""


templates.env.filters["timestamp_to_date"] = timestamp_to_date
# app.mount("/static", StaticFiles(directory="static"), name="static")

classifier = SentimentClassifier()
executor = ThreadPoolExecutor(max_workers=4)
r = redis.from_url(REDIS_URL, decode_responses=True)


@app.on_event("startup")
async def on_startup():
    await init_db()
    print("✅ Таблицы в БД созданы (если их ещё не было)")


def normalize_query(query: str) -> str:
    """Нормализует поисковый запрос: убирает лишние пробелы, приводит к нижнему регистру"""
    return query.strip().lower()


def make_cache_key(query: str, count: int) -> str:
    normalized_query = normalize_query(query)
    key_str = f"search:{normalized_query}:{count}"
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

    # Добавляем задержку перед каждым запросом
    await asyncio.sleep(0.25)

    async with ClientSession() as session:
        async with session.get(f"https://api.vk.com/method/{method}", params=params) as resp:
            data = await resp.json()
            if "error" in data:
                error = data['error']
                # Игнорируем ошибки доступа к комментариям и слишком частые запросы
                if error.get('error_code') in [212, 6]:
                    print(f"VK API Warning: {error['error_msg']} - пропускаем")
                    return {}
                print(f"VK API Error: {error}")
                return {}
            return data.get("response", {})


# ===== Helpers for day-based search =====
async def _classify_texts(texts: List[str]) -> Any:
    return await classify_texts_async(texts)


async def _fetch_vk_posts(query: str, start_dt: datetime, end_dt: datetime) -> List[dict]:
    resp = await vk_request("newsfeed.search", {
        "q": query,
        "count": 200,
        "extended": 1,
        "start_time": int(start_dt.timestamp()),
        "end_time": int(end_dt.timestamp())
    })
    return resp.get("items", []) if resp else []


async def _fetch_vk_comments(owner_id: int, post_id: int) -> List[dict]:
    resp = await vk_request("wall.getComments", {
        "owner_id": owner_id,
        "post_id": post_id,
        "count": 100
    })
    # Если запрос вернул пустой результат из-за ошибки, возвращаем пустой список
    if not resp:
        return []
    return resp.get("items", [])


def _build_summary(comments: List["Comment"]) -> Dict[str, int]:
    total_positive = sum(1 for c in comments if c.sentiment == "positive")
    total_negative = sum(1 for c in comments if c.sentiment == "negative")
    total = len(comments)
    total_neutral = max(0, total - total_positive - total_negative)
    return {"positive": total_positive, "negative": total_negative, "neutral": total_neutral, "total": total}


async def get_or_create_day_query(db: AsyncSession, query: str, days: int) -> "SearchQueryDay":
    normalized_query = normalize_query(query)
    end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(days=days)
    res = await db.execute(
        select(SearchQueryDay).where(
            (SearchQueryDay.query_text == normalized_query) &
            (SearchQueryDay.days == days) &
            (SearchQueryDay.start_date >= start_dt - timedelta(minutes=5)) &
            (SearchQueryDay.end_date <= end_dt + timedelta(minutes=5))
        )
    )
    dq = res.scalar_one_or_none()
    if dq:
        return dq
    dq = SearchQueryDay(
        query_text=normalized_query,
        days=days,
        start_date=start_dt,
        end_date=end_dt,
        task_id=None
    )
    db.add(dq)
    await db.flush()
    return dq


async def process_comments_async(task_id: str, query: str, count: int, cache_key: str):
    try:
        normalized_query = normalize_query(query)
        print(f"🚀 Начинаем обработку задачи {task_id} для запроса: {normalized_query}")
        # Обозначаем старт задачи, если ещё не отмечено
        await r.hset(f"task:{task_id}", mapping={"status": "processing"})
        async with AsyncSessionLocal() as db_session:
            posts_data = await vk_request("newsfeed.search",
                                          {"q": normalized_query, "count": min(count, 200), "extended": 1})
            if not posts_data:
                print("   ❌ Ответ от newsfeed.search пустой — проверь URL и токен")
                await r.hset(f"task:{task_id}", mapping={"status": "error", "error": "empty_response"})
                return
            posts = posts_data.get("items", [])
            print(f"   Найдено постов: {len(posts)}")

            expires_at = datetime.utcnow() + timedelta(seconds=CACHE_TTL)
            search_query = SearchQuery(
                query_text=normalized_query,
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


@app.post("/search_new", response_class=HTMLResponse)
async def search_posts_days(
        request: Request,
        query: str = Form(...),
        count: int = Form(10),
        db: AsyncSession = Depends(get_db)
):
    days = count
    result = await fetch_posts_for_last_days(db, query, days)
    comments_by_post = {}
    for c in result["comments"]:
        comments_by_post.setdefault(c.post_id, []).append(c)

    return templates.TemplateResponse("results_day.html", {
        "request": request,
        "query": result["day_query"].query_text,
        "days": result["day_query"].days,
        "posts": result["posts"],
        "comments_by_post": comments_by_post,
        "summary": result["summary"],
        "new_posts_count": result.get("new_posts_count", 0),
    })


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


async def fetch_posts_for_last_days(db: AsyncSession, query: str, days: int) -> Dict[str, Any]:
    normalized_query = normalize_query(query)
    end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(days=days)
    current_timestamp = int(end_dt.timestamp())

    # Определяем границы: сегодняшний день и предыдущие дни
    today_start = datetime(end_dt.year, end_dt.month, end_dt.day)
    yesterday_end = today_start - timedelta(seconds=1)

    # Разделяем период на две части: предыдущие дни и сегодня
    previous_days_start = start_dt
    previous_days_end = yesterday_end
    today_period = (today_start, end_dt)

    # Ищем существующие запросы для предыдущих дней
    existing_queries = await db.execute(
        select(SearchQueryDay).where(
            (SearchQueryDay.query_text == normalized_query) &
            (SearchQueryDay.start_date <= previous_days_end) &
            (SearchQueryDay.end_date >= previous_days_start)
        ).order_by(SearchQueryDay.days.desc())
    )
    existing_queries_list = existing_queries.scalars().all()

    # Определяем, какие периоды предыдущих дней уже есть в БД
    existing_coverage = []
    needed_previous_periods = [(previous_days_start, previous_days_end)]

    for eq in existing_queries_list:
        coverage_start = max(eq.start_date, previous_days_start)
        coverage_end = min(eq.end_date, previous_days_end)

        if coverage_start <= coverage_end:
            existing_coverage.append((coverage_start, coverage_end, eq))

            # Обновляем список нужных периодов для предыдущих дней
            new_needed_periods = []
            for needed_start, needed_end in needed_previous_periods:
                if coverage_start > needed_start:
                    new_needed_periods.append((needed_start, coverage_start))
                if coverage_end < needed_end:
                    new_needed_periods.append((coverage_end, needed_end))
            needed_previous_periods = new_needed_periods

    # Сегодняшний день всегда добавляем в нужные периоды для перепроверки
    needed_periods = needed_previous_periods + [today_period]

    # Создаем или находим основной запрос для текущего периода
    main_query = await db.execute(
        select(SearchQueryDay).where(
            (SearchQueryDay.query_text == normalized_query) &
            (SearchQueryDay.days == days) &
            (SearchQueryDay.start_date >= start_dt - timedelta(minutes=5)) &
            (SearchQueryDay.end_date <= end_dt + timedelta(minutes=5))
        )
    )
    day_query = main_query.scalar_one_or_none()

    if not day_query:
        day_query = SearchQueryDay(
            query_text=normalized_query,
            days=days,
            start_date=start_dt,
            end_date=end_dt,
            task_id=None
        )
        db.add(day_query)
        await db.flush()

    # Собираем посты из существующих запросов (только для предыдущих дней)
    all_posts = []
    existing_vk_ids = set()
    new_posts_count = 0

    if existing_coverage:
        # Получаем все посты из покрытых периодов предыдущих дней
        existing_post_ids_query = await db.execute(
            select(DayPost.post_id).where(
                DayPost.day_query_id.in_([eq.id for _, _, eq in existing_coverage])
            )
        )
        existing_post_ids = existing_post_ids_query.scalars().all()

        if existing_post_ids:
            # Фильтруем посты по актуальности для текущего периода
            posts_res = await db.execute(
                select(Post).where(
                    Post.id.in_(existing_post_ids) &
                    (Post.date >= int(start_dt.timestamp())) &
                    (Post.date <= current_timestamp)
                ).order_by(Post.date.desc())  # СОРТИРОВКА: от новых к старым
            )
            all_posts = posts_res.scalars().all()
            existing_vk_ids = {(p.owner_id, p.vk_post_id) for p in all_posts}

            # Связываем найденные посты с текущим запросом (если еще не связаны)
            existing_links = await db.execute(
                select(DayPost.post_id).where(
                    (DayPost.day_query_id == day_query.id) &
                    (DayPost.post_id.in_(existing_post_ids))
                )
            )
            existing_linked_ids = set(existing_links.scalars().all())

            for post in all_posts:
                if post.id not in existing_linked_ids:
                    db.add(DayPost(day_query_id=day_query.id, post_id=post.id))

    # Загружаем посты для недостающих периодов (включая сегодняшний день)
    all_comments = []
    all_texts = []

    for period_start, period_end in needed_periods:
        # Разбиваем период на однодневные отрезки
        current_day_start = period_start
        daily_segments = []

        while current_day_start < period_end:
            current_day_end = min(
                datetime(current_day_start.year, current_day_start.month, current_day_start.day) + timedelta(days=1),
                period_end
            )
            daily_segments.append((current_day_start, current_day_end))
            current_day_start = current_day_end

        # Загружаем посты для каждого дня отдельно и накапливаем в new_items
        new_items = []
        for day_start, day_end in daily_segments:
            print(
                f"🔍 Загружаем посты за период: {day_start.strftime('%d.%m.%Y %H:%M')} - {day_end.strftime('%d.%m.%Y %H:%M')}")

            day_items = await _fetch_vk_posts(normalized_query, day_start, day_end)
            print(f"   Найдено постов за этот день: {len(day_items)}")

            # Добавляем посты дня к общему списку new_items
            new_items.extend(day_items)

        print(f"📊 Всего загружено постов за период: {len(new_items)}")

        # Обрабатываем все накопленные посты
        for post in new_items:
            owner_id = post.get("owner_id")
            vk_post_id = post.get("id")
            if owner_id is None or vk_post_id is None:
                continue

            if (owner_id, vk_post_id) in existing_vk_ids:
                continue

            # Ищем пост в БД
            res_post = await db.execute(
                select(Post).where(
                    (Post.owner_id == owner_id) &
                    (Post.vk_post_id == vk_post_id)
                )
            )
            db_post = res_post.scalar_one_or_none()

            if not db_post:
                db_post = Post(
                    vk_post_id=vk_post_id,
                    owner_id=owner_id,
                    text=post.get("text", "")[:5000],
                    date=post.get("date"),
                    url=f"https://vk.com/wall{owner_id}_{vk_post_id}",
                    search_query_id=None
                )
                db.add(db_post)
                await db.flush()
                new_posts_count += 1

            # Связываем с текущим запросом
            db.add(DayPost(day_query_id=day_query.id, post_id=db_post.id))
            all_posts.append(db_post)
            existing_vk_ids.add((owner_id, vk_post_id))

            # Загружаем комментарии
            try:
                comments = await _fetch_vk_comments(owner_id, vk_post_id)
                for c in comments:
                    text = (c.get("text") or "").strip()
                    if not text:
                        continue

                    # Проверяем существование комментария
                    existing_comment = await db.execute(
                        select(Comment.id).where(
                            (Comment.vk_comment_id == c["id"]) &
                            (Comment.post_id == db_post.id)
                        )
                    )
                    if not existing_comment.first():
                        all_comments.append({
                            "comment": c,
                            "post_id": db_post.id,
                            "text": text
                        })
                        all_texts.append(text)
            except Exception as e:
                print(f"Ошибка при загрузке комментариев к посту {owner_id}_{vk_post_id}: {e}")
                continue

    # Пакетная классификация и сохранение комментариев
    if all_texts:
        labels, confidences = await _classify_texts(all_texts)

        for i, item in enumerate(all_comments):
            if i >= len(labels):
                break

            c = item["comment"]
            db.add(Comment(
                vk_comment_id=c["id"],
                post_id=item["post_id"],
                from_id=c.get("from_id"),
                text=item["text"][:2000],
                sentiment=labels[i],
                sentiment_confidence=float(confidences[i]),
                date=c.get("date")
            ))

    await db.commit()

    # Получаем все комментарии для возвращаемых постов
    post_ids = [post.id for post in all_posts]
    all_comments_result = []
    if post_ids:
        comments_res = await db.execute(
            select(Comment).where(Comment.post_id.in_(post_ids))
        )
        all_comments_result = comments_res.scalars().all()

    # Создаем summary для комментариев
    summary = _build_summary(all_comments_result)

    # СОРТИРУЕМ ПОСТЫ ОТ НОВЫХ К СТАРЫМ
    all_posts_sorted = sorted(all_posts, key=lambda x: x.date if x.date else 0, reverse=True)

    return {
        "day_query": day_query,
        "posts": all_posts_sorted,  # Возвращаем отсортированные посты
        "comments": all_comments_result,
        "summary": summary,
        "new_posts_count": new_posts_count,
        "total_posts": len(all_posts),
        "coverage_periods": existing_coverage,
        "fetched_periods": needed_periods
    }