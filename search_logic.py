# search_logic.py
import asyncio
from typing import List, Optional
from datetime import datetime, timedelta, timezone
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from config import CACHE_TTL
from models import SearchQuery, Post, Comment
from utils import vk_request, classify_texts_async


async def create_initial_search_query(db: AsyncSession, query: str, count: int, task_id: str):
    expires_at = datetime.utcnow() + timedelta(seconds=CACHE_TTL)
    initial_search_query = SearchQuery(
        query_text=query,
        count=count,
        task_id=task_id,
        expires_at=expires_at
    )
    db.add(initial_search_query)
    await db.commit()
    await db.refresh(initial_search_query)
    return initial_search_query


async def get_search_task_status(db: AsyncSession, task_id: str) -> dict:
    search_query_result = await db.execute(select(SearchQuery).where(SearchQuery.task_id == task_id))
    search_query = search_query_result.scalar_one_or_none()
    if not search_query:
        return {"status": "not_found"}

    posts_result = await db.execute(select(Post).where(Post.search_query_id == search_query.id))
    posts = posts_result.scalars().all()

    if len(posts) > 0:
        return {"status": "done"}

    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    time_diff = now_utc - search_query.created_at.replace(tzinfo=None)
    if time_diff.total_seconds() > 600:  # 10 минут
        return {"status": "error", "error": "timeout or empty result"}
    else:
        return {"status": "processing"}


async def get_search_results(db: AsyncSession, task_id: str):
    result = await db.execute(select(SearchQuery).where(SearchQuery.task_id == task_id))
    search_query = result.scalar_one_or_none()
    if not search_query:
        return None

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

    return {
        "query": search_query.query_text,
        "posts": posts,
        "comments_by_post": comments_by_post,
        "all_comments": all_comments,
        "summary": {
            "positive": total_positive,
            "negative": total_negative,
            "total": len(all_comments)
        }
    }


async def process_comments_async(task_id: str, query: str, count: int, cache_key: str):
    """
    Основная функция для поиска постов и комментариев,
    их классификации и сохранения в БД.
    """
    from config import CACHE_TTL
    from datetime import datetime, timedelta

    try:
        print(f"🚀 Начинаем обработку задачи {task_id} для запроса: {query}")
        async with AsyncSessionLocal() as db_session:
            search_query_result = await db_session.execute(select(SearchQuery).where(SearchQuery.task_id == task_id))
            existing_search_query = search_query_result.scalar_one_or_none()
            if existing_search_query:
                print(f"   📝 Обновляем существующий SearchQuery с ID: {existing_search_query.id}")
                existing_search_query.expires_at = datetime.utcnow() + timedelta(seconds=CACHE_TTL)
                existing_search_query.count = count
                search_query = existing_search_query
            else:
                print(f"   🆕 Создаём новый SearchQuery с task_id: {task_id}")
                expires_at = datetime.utcnow() + timedelta(seconds=CACHE_TTL)
                search_query = SearchQuery(
                    query_text=query,
                    count=count,
                    task_id=task_id,
                    expires_at=expires_at
                )
                db_session.add(search_query)
            await db_session.flush()

            posts_data = await vk_request("newsfeed.search", {"q": query, "count": min(count, 200), "extended": 1})
            if not posts_data:
                print("   ❌ Ответ от newsfeed.search пустой — проверь URL и токен")
                return

            posts = posts_data.get("items", [])
            print(f"   Найдено постов: {len(posts)}")
            if not posts:
                print("   ❌ Нет постов — завершаем задачу (пустые результаты)")
                await db_session.commit()
                return

            all_comments = []
            all_texts = []
            post_cache = {}

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
            print(f"✅ Задача {task_id} успешно завершена")

    except Exception as e:
        print(f"❌ Ошибка в задаче {task_id}: {e}")