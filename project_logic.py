# project_logic.py
import asyncio
from datetime import datetime, timedelta # Убран timezone
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
from models import Project, SearchQuery, Post, Comment
from utils import vk_request, classify_texts_async # Предполагаем, что эти функции находятся в utils.py
from config import CACHE_TTL # Предполагаем, что CACHE_TTL определена в config.py

# --- Функции для работы с проектами ---
async def create_project(db: AsyncSession, name: str, search_depth_days: int) -> Project:
    """Создает новый проект в БД."""
    project = Project(name=name, search_depth_days=search_depth_days)
    db.add(project)
    await db.commit()
    await db.refresh(project)
    return project

async def get_all_projects(db: AsyncSession) -> list[Project]:
    """Получает список всех проектов из БД."""
    result = await db.execute(select(Project))
    return result.scalars().all()

async def get_project_by_id(db: AsyncSession, project_id: int) -> Project | None:
    """Получает проект по ID из БД."""
    result = await db.execute(select(Project).where(Project.id == project_id))
    return result.scalar_one_or_none()

async def update_project(db: AsyncSession, project_id: int, name: str, search_depth_days: int) -> bool:
    """Обновляет проект в БД."""
    project = await get_project_by_id(db, project_id)
    if not project:
        return False
    project.name = name
    project.search_depth_days = search_depth_days
    await db.commit()
    return True

# --- НОВАЯ ФУНКЦИЯ: Удаление проекта ---
async def delete_project(db: AsyncSession, project_id: int) -> bool:
    """Удаляет проект и связанные с ним SearchQuery, Post и Comment из БД."""
    project = await get_project_by_id(db, project_id)
    if not project:
        print(f"❌ Проект с ID {project_id} не найден для удаления.")
        return False

    # Удаляем проект. Благодаря ondelete="CASCADE" в моделях,
    # связанные SearchQuery, Post и Comment будут удалены автоматически.
    await db.delete(project)
    await db.commit()
    print(f"✅ Проект с ID {project_id} и связанные данные успешно удалены из БД.")
    return True

async def run_project_search(db: AsyncSession, project_id: int):
    """
    Запускает поиск для проекта.
    Проверяет наличие существующих данных в БД за указанный период.
    """
    project = await get_project_by_id(db, project_id)
    if not project:
        print(f"❌ Проект с ID {project_id} не найден.")
        return

    query_text = project.name
    depth_days = project.search_depth_days
    print(f"🚀 Запуск поиска для проекта '{query_text}' за {depth_days} дней.")

    # Вычисляем диапазон дат
    # --- ИСПОЛЬЗУЕМ offset-naive datetime ---
    now = datetime.utcnow() # offset-naive
    start_date = int((now - timedelta(days=depth_days)).timestamp())
    end_date = int(now.timestamp())

    # Проверяем, есть ли уже SearchQuery для этого диапазона и запроса
    existing_query_result = await db.execute(
        select(SearchQuery)
        .where(
            and_(
                SearchQuery.query_text == query_text,
                # --- ИСПОЛЬЗУЕМ offset-naive datetime ---
                SearchQuery.created_at >= datetime.fromtimestamp(start_date), # offset-naive
                SearchQuery.created_at <= datetime.fromtimestamp(end_date)  # offset-naive
            )
        )
        .order_by(SearchQuery.created_at.desc()) # Берем самый новый за период
    )
    existing_search_query = existing_query_result.scalar_one_or_none()

    if existing_search_query:
        print(f"   💾 Используем существующий SearchQuery ID: {existing_search_query.id}")
        search_query = existing_search_query
    else:
        print("   📥 Выполняем новый поиск через API VK, по дням...")

        # --- НОВАЯ ЛОГИКА: Цикличный поиск по дням ---
        current_end_time = end_date
        all_filtered_posts = []
        total_posts_fetched = 0

        for day in range(depth_days):
            # --- ИСПОЛЬЗУЕМ offset-naive datetime ---
            current_start_time = int((datetime.fromtimestamp(current_end_time) - timedelta(days=1)).timestamp()) # offset-naive

            print(f"   📥 Ищем посты с {datetime.fromtimestamp(current_start_time)} по {datetime.fromtimestamp(current_end_time)}") # offset-naive

            posts_data = await vk_request("newsfeed.search", {
                "q": query_text,
                "start_time": current_start_time,
                "end_time": current_end_time,
                "count": 200, # Максимум за один запрос
                "extended": 1
            })

            # --- ИСПРАВЛЕНО: posts_data -> posts_data ---
            if not posts_data:
                print(f"   ❌ Ответ от newsfeed.search пустой для диапазона {current_start_time}-{current_end_time}")
                current_end_time = current_start_time
                continue # Переходим к следующему дню

            posts = posts_data.get("items", [])
            total_posts_fetched += len(posts)
            print(f"   Найдено постов за день {datetime.fromtimestamp(current_start_time).date()}: {len(posts)}") # offset-naive

            # Фильтруем посты по дате (на всякий случай, если API вернул что-то за пределами диапазона)
            filtered_posts_for_day = [p for p in posts if current_start_time <= p.get("date", 0) <= current_end_time]
            all_filtered_posts.extend(filtered_posts_for_day)

            current_end_time = current_start_time # Переходим к предыдущему дню

        print(f"   Всего постов за {depth_days} дней: {len(all_filtered_posts)} (запрошено: {total_posts_fetched})")

        # --- КОНЕЦ НОВОЙ ЛОГИКИ ---

        if not all_filtered_posts:
            print("   ❌ Нет постов за указанный период — завершаем задачу (пустые результаты)")
            # Создаем пустой SearchQuery для отслеживания попытки поиска
            # --- ИСПОЛЬЗУЕМ offset-naive datetime для expires_at ---
            expires_at = datetime.utcnow() + timedelta(seconds=CACHE_TTL) # offset-naive
            search_query = SearchQuery(
                query_text=query_text,
                count=0,
                # --- ИСПОЛЬЗУЕМ offset-naive datetime для created_at ---
                created_at=datetime.fromtimestamp(end_date), # offset-naive, используем дату окончания
                expires_at=expires_at, # offset-naive
                task_id=None # Проекты не используют task_id напрямую
            )
            db.add(search_query)
            await db.commit()
            print(f"   ✅ Пустой SearchQuery для проекта '{query_text}' сохранен в DB.")
            return

        # --- Обработка найденных постов ---
        # --- ИСПОЛЬЗУЕМ offset-naive datetime для expires_at ---
        expires_at = datetime.utcnow() + timedelta(seconds=CACHE_TTL) # offset-naive
        # Используем дату самого нового поста (первого в списке, т.к. VK возвращает от новых к старым)
        # --- ИСПОЛЬЗУЕМ offset-naive datetime для created_at ---
        newest_post_date = datetime.fromtimestamp(all_filtered_posts[0]['date']) if all_filtered_posts else datetime.utcnow() # offset-naive
        search_query = SearchQuery(
            query_text=query_text,
            count=len(all_filtered_posts), # Используем фильтрованное количество
            created_at=newest_post_date, # offset-naive
            expires_at=expires_at, # offset-naive
            task_id=None # Проекты не используют task_id напрямую
        )
        db.add(search_query)
        await db.flush() # Получаем ID

        post_cache = {}
        all_comments = []
        all_texts = []

        for post in all_filtered_posts:
            post_date = post.get("date")
            # Фильтрация по дате уже выполнена выше при сборе all_filtered_posts
            owner_id = post["owner_id"]
            post_id = post["id"]
            if (owner_id, post_id) not in post_cache:
                db_post = Post(
                    vk_post_id=post_id,
                    owner_id=owner_id,
                    text=post.get("text", "")[:5000],
                    date=post_date,
                    url=f"https://vk.com/wall{owner_id}_{post_id}",
                    search_query_id=search_query.id
                )
                db.add(db_post)
                await db.flush()
                post_cache[(owner_id, post_id)] = db_post.id

            # Загружаем комментарии к посту
            comments_data = await vk_request("wall.getComments", {
                "owner_id": owner_id,
                "post_id": post_id,
                "count": 100
            })
            comments = comments_data.get("items", [])
            for comment in comments:
                comment_date = comment.get("date")
                # Фильтруем комментарии по дате (в рамках периода проекта)
                if start_date <= comment_date <= end_date:
                    text = comment.get("text", "").strip()
                    if text:
                        all_comments.append({
                            "comment": comment,
                            "owner_id": owner_id,
                            "post_id": post_id,
                            "post_id_db": post_cache[(owner_id, post_id)]
                        })
                        all_texts.append(text)

        if all_texts:
            labels, confidences = await classify_texts_async(all_texts)
            for i, item in enumerate(all_comments):
                if i >= len(labels):
                    break
                comment = item["comment"]
                db_comment = Comment(
                    vk_comment_id=comment["id"],
                    post_id=item["post_id_db"], # Используем ID из БД
                    from_id=comment.get("from_id"),
                    text=comment["text"][:2000],
                    sentiment=labels[i],
                    sentiment_confidence=float(confidences[i]),
                    date=comment.get("date")
                )
                db.add(db_comment)

        if all_texts:
            print(f"   Сохранено комментариев: {len(all_texts)}")
        else:
            print("   ❌ Нет комментариев для сохранения за указанный период.")

        await db.commit()
        print(f"   ✅ Новые данные для проекта '{query_text}' сохранены в DB.")


async def get_project_stats(db: AsyncSession, search_query_id: int):
    """Получает статистику по комментариям для конкретного SearchQuery."""
    posts_result = await db.execute(select(Post).where(Post.search_query_id == search_query_id))
    posts = posts_result.scalars().all()
    post_ids = [post.id for post in posts]
    comments_result = await db.execute(select(Comment).where(Comment.post_id.in_(post_ids)))
    all_comments = comments_result.scalars().all()

    total_positive = sum(1 for c in all_comments if c.sentiment == "positive")
    total_negative = sum(1 for c in all_comments if c.sentiment == "negative")
    total_comments = len(all_comments)

    return {
        "positive": total_positive,
        "negative": total_negative,
        "total": total_comments,
        "posts_count": len(posts)
    }

# Другие функции, связанные с логикой проектов, можно добавить сюда при необходимости.