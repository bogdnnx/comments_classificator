# search_logic.py
import asyncio
from typing import List
from utils import vk_request, classify_texts_async, r # Импортируем зависимости из utils
from database import AsyncSessionLocal # Импортируем сессию из database
from models import SearchQuery, Post, Comment # Предполагается существование файла models.py
from datetime import datetime, timedelta
import uuid # Импортируем uuid для генерации ID


async def process_comments_async(task_id: str, query: str, count: int, cache_key: str):
    """
    Основная функция для поиска постов и комментариев,
    их классификации и сохранения в БД.
    """
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

            from utils import CACHE_TTL # Импорт внутри функции
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
        await r.hset