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
    Всегда перепроверяет сегодняшний день.
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
    today_start = int(datetime(now.year, now.month, now.day).timestamp()) # Начало сегодняшнего дня (00:00:00 UTC)
    today_end = int(now.timestamp()) # Конец сегодняшнего дня (сейчас)

    # Вычисляем диапазон для проверки кэша (вчера - depth_days + 1, включая сегодня)
    # Например, если depth = 3, то кэш проверяется для: (сегодня - 2 дня) до сегодня
    # start_date для кэша = сегодня - 2 дня = today_start - (2 * 86400)
    # end_date для кэша = today_end
    cache_start_date = today_start - (depth_days - 1) * 86400 # 86400 секунд в дне

    # --- НОВАЯ ЛОГИКА: Цикличный поиск по дням ---
    all_filtered_posts = []
    total_posts_fetched = 0
    post_cache = {} # Для отслеживания ID постов, чтобы избежать дубликатов
    all_comments = []
    all_texts = []

    # 1. Проверяем кэш для дней, кроме сегодняшнего
    # Ищем *любой* SearchQuery, который пересекается с периодом, НЕ включая сегодня
    existing_query_result = await db.execute(
        select(SearchQuery)
        .where(
            and_(
                SearchQuery.query_text == query_text,
                SearchQuery.created_at >= datetime.fromtimestamp(cache_start_date), # offset-naive
                SearchQuery.created_at <= datetime.fromtimestamp(today_start - 1)   # offset-naive, до начала сегодняшнего дня
            )
        )
        .order_by(SearchQuery.created_at.desc()) # Берем самый новый за период до сегодня
    )
    existing_search_query = existing_query_result.scalar_one_or_none()

    if existing_search_query:
        print(f"   💾 Найден кэш для дней до сегодняшнего (ID: {existing_search_query.id}).")
        # Загружаем посты из кэша
        posts_result = await db.execute(select(Post).where(Post.search_query_id == existing_search_query.id))
        cached_posts = posts_result.scalars().all()
        for post in cached_posts:
            post_cache[(post.owner_id, post.vk_post_id)] = post.id
        print(f"   📥 Загружено {len(cached_posts)} постов из кэша.")

    # 2. Выполняем поиск по дням, включая сегодняшний день
    current_end_time = today_end
    for day in range(depth_days):
        # --- ИСПОЛЬЗУЕМ offset-naive datetime ---
        current_start_time = int((datetime.fromtimestamp(current_end_time) - timedelta(days=1)).timestamp()) # offset-naive

        # Если это сегодняшний день, всегда делаем запрос
        if current_start_time >= today_start:
            print(f"   🔄 Перепроверка сегодняшнего дня: {datetime.fromtimestamp(current_start_time).date()}")
        else:
            print(f"   📥 Запрос к API за день: {datetime.fromtimestamp(current_start_time).date()}")

        posts_data = await vk_request("newsfeed.search", {
            "q": query_text,
            "start_time": current_start_time,
            "end_time": current_end_time,
            "count": 50, # Максимум за один запрос
            "extended": 1
        })

        if not posts_data: # --- ИСПРАВЛЕНО: posts_data -> posts_data ---
            print(f"   ❌ Ответ от newsfeed.search пустой для диапазона {current_start_time}-{current_end_time}")
            current_end_time = current_start_time
            continue # Переходим к следующему дню

        posts = posts_data.get("items", [])
        total_posts_fetched += len(posts)
        print(f"   Найдено постов за день {datetime.fromtimestamp(current_start_time).date()}: {len(posts)}")

        # Фильтруем посты по дате и ID, чтобы избежать дубликатов
        for post in posts:
            post_date = post.get("date", 0)
            if current_start_time <= post_date <= current_end_time:
                owner_id = post["owner_id"]
                post_id = post["id"]
                if (owner_id, post_id) not in post_cache:
                    all_filtered_posts.append(post)
                    post_cache[(owner_id, post_id)] = None # Заглушка, будет заполнена позже

        current_end_time = current_start_time # Переходим к предыдущему дню

    print(f"   Всего новых/уникальных постов за {depth_days} дней (включая сегодня): {len(all_filtered_posts)} (запрошено: {total_posts_fetched})")

    # --- Обработка найденных постов (новых или обновленных) ---
    # Если есть существующий кэш, используем его SearchQuery, иначе создаем новый
    if existing_search_query:
        print(f"   📝 Обновляем существующий SearchQuery ID: {existing_search_query.id}")
        search_query = existing_search_query
    else:
        print(f"   🆕 Создаем новый SearchQuery")
        # --- ИСПОЛЬЗУЕМ offset-naive datetime для expires_at ---
        expires_at = datetime.utcnow() + timedelta(seconds=CACHE_TTL) # offset-naive
        # Используем дату самого нового поста (первого в списке, т.к. VK возвращает от новых к старым)
        # или время запуска, если постов не было
        newest_post_date = datetime.fromtimestamp(all_filtered_posts[0]['date']) if all_filtered_posts else datetime.utcnow() # offset-naive
        search_query = SearchQuery(
            query_text=query_text,
            count=len(all_filtered_posts), # Используем фильтрованное количество новых
            created_at=newest_post_date, # offset-naive
            expires_at=expires_at, # offset-naive
            task_id=None # Проекты не используют task_id напрямую
        )
        db.add(search_query)
        await db.flush() # Получаем ID

    # Обновляем кэш с новыми постами и сохраняем их в БД
    for post in all_filtered_posts:
        post_date = post.get("date")
        owner_id = post["owner_id"]
        post_id = post["id"]
        if (owner_id, post_id) not in post_cache or post_cache[(owner_id, post_id)] is None: # Проверяем, что пост новый
            db_post = Post(
                vk_post_id=post_id,
                owner_id=owner_id,
                text=post.get("text", "")[:5000],
                date=post_date,
                url=f"https://vk.com/wall{owner_id}_{post_id}",
                search_query_id=search_query.id
            )
            db.add(db_post)
            await db.flush() # Получаем ID нового поста
            post_cache[(owner_id, post_id)] = db_post.id # Обновляем кэш

    # --- Загрузка и обновление комментариев ---
    # Загружаем *все* посты, связанные с текущим SearchQuery (включая кэшированные и новые)
    all_posts_result = await db.execute(select(Post).where(Post.search_query_id == search_query.id))
    all_posts_in_query = all_posts_result.scalars().all()
    print(f"   📥 Загружено {len(all_posts_in_query)} постов для обновления комментариев.")

    for post in all_posts_in_query:
        # Загружаем комментарии к посту
        comments_data = await vk_request("wall.getComments", {
            "owner_id": post.owner_id,
            "post_id": post.vk_post_id,
            "count": 100
        })
        comments = comments_data.get("items", [])
        for comment in comments:
            comment_date = comment.get("date")
            # Фильтруем комментарии по дате (в рамках периода проекта)
            if cache_start_date <= comment_date <= today_end: # Проверяем диапазон с начала кэша до конца сегодня
                text = comment.get("text", "").strip()
                if text:
                    # Проверяем, есть ли уже комментарий с таким ID в БД для этого поста
                    existing_comment_result = await db.execute(
                        select(Comment).where(
                            and_(
                                Comment.vk_comment_id == comment["id"],
                                Comment.post_id == post.id
                            )
                        )
                    )
                    existing_comment = existing_comment_result.scalar_one_or_none()
                    if not existing_comment: # Если комментарий новый
                        all_comments.append({
                            "comment": comment,
                            "post_id_db": post.id
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
        print(f"   Сохранено/обновлено {len(all_texts)} комментариев.")
    else:
        print("   ❌ Нет новых/обновленных комментариев для сохранения за указанный период.")

    await db.commit()
    print(f"   ✅ Данные для проекта '{query_text}' за {depth_days} дней (включая сегодня) обновлены в DB.")


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

    # --- НОВАЯ ЛОГИКА: Получение постов с наибольшим количеством комментариев ---
    comments_by_post_id = {}
    for comment in all_comments:
        comments_by_post_id.setdefault(comment.post_id, []).append(comment)

    # Сортируем посты по количеству комментариев (по убыванию)
    sorted_posts_with_comments = sorted(
        posts,
        key=lambda p: len(comments_by_post_id.get(p.id, [])),
        reverse=True
    )

    # Берем топ 5 постов (или меньше, если их меньше)
    top_posts_with_comments = sorted_posts_with_comments[:5]

    return {
        "positive": total_positive,
        "negative": total_negative,
        "total": total_comments,
        "posts_count": len(posts),
        "top_posts": top_posts_with_comments, # Возвращаем список постов
        "comments_by_post": comments_by_post_id # Возвращаем словарь комментариев для этих постов
    }

# Другие функции, связанные с логикой проектов, можно добавить сюда при необходимости.