# project_logic.py
import asyncio
from datetime import datetime, timedelta
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
from models import Project, SearchQuery, Post, Comment, ProjectSearchQuery, ProjectComment
from utils import vk_request, classify_texts_async,fetch_comments_via_execute
from config import CACHE_TTL


# --- CRUD для проектов ---
async def create_project(db: AsyncSession, name: str, search_depth_days: int) -> Project:
    project = Project(name=name, search_depth_days=search_depth_days)
    db.add(project)
    await db.commit()
    await db.refresh(project)
    return project


async def get_all_projects(db: AsyncSession) -> list[Project]:
    result = await db.execute(select(Project))
    return result.scalars().all()


async def get_project_by_id(db: AsyncSession, project_id: int) -> Project | None:
    result = await db.execute(select(Project).where(Project.id == project_id))
    return result.scalar_one_or_none()


async def update_project(db: AsyncSession, project_id: int, name: str, search_depth_days: int) -> bool:
    project = await get_project_by_id(db, project_id)
    if not project:
        return False
    project.name = name
    project.search_depth_days = search_depth_days
    await db.commit()
    return True


async def delete_project(db: AsyncSession, project_id: int) -> bool:
    project = await get_project_by_id(db, project_id)
    if not project:
        print(f"❌ Проект с ID {project_id} не найден для удаления.")
        return False
    await db.delete(project)
    await db.commit()
    print(f"✅ Проект с ID {project_id} и связанные данные успешно удалены.")
    return True


# --- Основная функция поиска для проекта ---
async def run_project_search(db: AsyncSession, project_id: int):
    project = await get_project_by_id(db, project_id)
    if not project:
        print(f"❌ Проект с ID {project_id} не найден.")
        return

    query_text = project.name
    depth_days = project.search_depth_days
    print(f"🚀 Запуск поиска для проекта '{query_text}' (ID: {project_id}) за {depth_days} дней.")

    # --- Даты поиска (offset-naive) ---
    now = datetime.utcnow()
    today_start = int(datetime(now.year, now.month, now.day).timestamp())
    today_end = int(now.timestamp())
    search_start_date = today_start - (depth_days - 1) * 86400

    # --- Поиск/создание SearchQuery через ProjectSearchQuery ---
    project_query_link_result = await db.execute(
        select(ProjectSearchQuery)
        .where(ProjectSearchQuery.project_id == project_id)
        .order_by(ProjectSearchQuery.id.desc())
    )
    project_query_link = project_query_link_result.scalar_one_or_none()

    if project_query_link:
        search_query_result = await db.execute(
            select(SearchQuery).where(SearchQuery.id == project_query_link.search_query_id)
        )
        search_query = search_query_result.scalar_one_or_none()
        if not search_query:
            print(f"❌ Связанный SearchQuery не найден. Создаём новый.")
            expires_at = datetime.utcnow() + timedelta(seconds=CACHE_TTL)
            search_query = SearchQuery(
                query_text=query_text,
                count=0,
                created_at=datetime.utcnow(),
                expires_at=expires_at,
                task_id=f"project_{project_id}"
            )
            db.add(search_query)
            await db.flush()
            project_query_link.search_query_id = search_query.id
            db.add(project_query_link)
        else:
            print(f"   📥 Используем существующий SearchQuery (ID: {search_query.id})")
    else:
        print(f"   🆕 Создаём новый SearchQuery для проекта {project_id}")
        expires_at = datetime.utcnow() + timedelta(seconds=CACHE_TTL)
        search_query = SearchQuery(
            query_text=query_text,
            count=0,
            created_at=datetime.utcnow(),
            expires_at=expires_at,
            task_id=f"project_{project_id}"
        )
        db.add(search_query)
        await db.flush()
        project_query_link = ProjectSearchQuery(project_id=project_id, search_query_id=search_query.id)
        db.add(project_query_link)

    # --- Поиск постов по дням ---
    all_filtered_posts = []
    post_cache = {}
    current_end_time = today_end

    for day in range(depth_days):
        current_start_time = today_start - day * 86400  # день N назад (0 = сегодня)
        current_end_time = current_start_time + 86400
        print(f"   📥 Ищем посты с {datetime.fromtimestamp(current_start_time).date()} по {datetime.fromtimestamp(current_end_time).date()}")
        posts_data = await vk_request("newsfeed.search", {
            "q": query_text,
            "start_time": current_start_time,
            "end_time": current_end_time,
            "count": 200,
            "extended": 1
        })
        if not posts_data:
            print(f"   ❌ Пустой ответ от VK для диапазона {current_start_time}-{current_end_time}")
            current_end_time = current_start_time
            continue

        posts = posts_data.get("items", [])
        print(f"   Найдено постов за день {datetime.fromtimestamp(current_start_time).date()}: {len(posts)}")

        for post in posts:
            post_date = post.get("date", 0)
            if current_start_time <= post_date <= current_end_time:
                owner_id = post["owner_id"]
                post_id = post["id"]
                if (owner_id, post_id) not in post_cache:
                    all_filtered_posts.append(post)
                    post_cache[(owner_id, post_id)] = None
        current_end_time = current_start_time

    print(f"   Всего уникальных постов: {len(all_filtered_posts)}")

    # --- Сохранение постов ---
    for post in all_filtered_posts:
        owner_id = post["owner_id"]
        post_id = post["id"]
        post_date = post.get("date")

        existing_post_result = await db.execute(
            select(Post).where(
                and_(
                    Post.vk_post_id == post_id,
                    Post.owner_id == owner_id,
                    Post.search_query_id == search_query.id
                )
            )
        )
        existing_post = existing_post_result.scalar_one_or_none()

        if not existing_post:
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
        else:
            post_cache[(owner_id, post_id)] = existing_post.id

    # --- Обработка комментариев (включая существующие!) ---
    all_comments_to_classify = []
    all_texts_to_classify = []

    # 1. Собираем список постов для запроса комментариев
    posts_for_comments = [(post["owner_id"], post["id"]) for post in all_filtered_posts]
    print(f"   📥 Запрашиваем комментарии к {len(posts_for_comments)} постам через execute...")

    # 2. Получаем ВСЕ комментарии одним/несколькими execute-запросами
    all_vk_comments = await fetch_comments_via_execute(posts_for_comments)

    # 3. Обрабатываем каждый полученный комментарий
    for item in all_vk_comments:
        owner_id = item["owner_id"]
        post_id_vk = item["post_id"]
        comment = item["comment"]

        # Находим ID поста в БД (уже сохранён ранее)
        db_post_id = post_cache.get((owner_id, post_id_vk))
        if db_post_id is None:
            continue  # Не должно происходить, но на всякий случай

        comment_date = comment.get("date", 0)
        text = comment.get("text", "").strip()
        if not text:
            continue
        if not (search_start_date <= comment_date <= today_end):
            continue

        # Проверяем, существует ли комментарий в БД
        existing_comment_result = await db.execute(
            select(Comment).where(
                and_(
                    Comment.vk_comment_id == comment["id"],
                    Comment.post_id == db_post_id
                )
            )
        )
        existing_comment = existing_comment_result.scalar_one_or_none()

        if existing_comment:
            # Привязываем СУЩЕСТВУЮЩИЙ комментарий к проекту, если ещё не привязан
            proj_comment_result = await db.execute(
                select(ProjectComment).where(
                    and_(
                        ProjectComment.project_id == project_id,
                        ProjectComment.comment_id == existing_comment.id
                    )
                )
            )
            if not proj_comment_result.scalar_one_or_none():
                db.add(ProjectComment(project_id=project_id, comment_id=existing_comment.id))
        else:
            # Новый комментарий — добавляем в очередь на классификацию
            all_comments_to_classify.append({"comment": comment, "post_id_db": db_post_id})
            all_texts_to_classify.append(text)
    # --- Классификация и сохранение новых комментариев ---
    if all_texts_to_classify:
        labels, confidences = await classify_texts_async(all_texts_to_classify)
        for i, item in enumerate(all_comments_to_classify):
            if i >= len(labels):
                break
            comment = item["comment"]
            db_comment = Comment(
                vk_comment_id=comment["id"],
                post_id=item["post_id_db"],
                from_id=comment.get("from_id"),
                text=comment["text"][:2000],
                sentiment=labels[i],
                sentiment_confidence=float(confidences[i]),
                date=comment.get("date")
            )
            db.add(db_comment)
            await db.flush()
            db.add(ProjectComment(project_id=project_id, comment_id=db_comment.id))

    # --- Финал ---
    search_query.count = len(all_filtered_posts)
    await db.commit()
    print(f"   ✅ Поиск для проекта '{query_text}' завершён. Все релевантные комментарии привязаны к проекту.")


# --- Статистика проекта ---
async def get_project_stats(db: AsyncSession, project_id: int):
    project = await get_project_by_id(db, project_id)
    if not project:
        print(f"❌ Проект с ID {project_id} не найден.")
        return {
            "positive": 0,
            "negative": 0,
            "total": 0,
            "posts_count": 0,
            "top_posts": [],
            "comments_by_post": {}
        }

    # --- 1. Найдём SearchQuery, связанный с проектом ---
    project_query_link_result = await db.execute(
        select(ProjectSearchQuery.search_query_id)
        .where(ProjectSearchQuery.project_id == project_id)
        .order_by(ProjectSearchQuery.id.desc())
    )
    search_query_id_row = project_query_link_result.scalar_one_or_none()

    if not search_query_id_row:
        print(f"❌ Нет связанного SearchQuery для проекта {project_id}.")
        return {
            "positive": 0,
            "negative": 0,
            "total": 0,
            "posts_count": 0,
            "top_posts": [],
            "comments_by_post": {}
        }

    search_query_id = search_query_id_row

    # --- 2. Считаем ВСЕ посты, связанные с этим SearchQuery ---
    posts_result = await db.execute(
        select(Post).where(Post.search_query_id == search_query_id)
    )
    all_posts = posts_result.scalars().all()
    posts_count = len(all_posts)
    post_ids = [p.id for p in all_posts]

    # --- 3. Считаем комментарии ТОЛЬКО через ProjectComment ---
    project_comment_ids_result = await db.execute(
        select(ProjectComment.comment_id).where(ProjectComment.project_id == project_id)
    )
    project_comment_ids = [row.comment_id for row in project_comment_ids_result.all()]

    if not project_comment_ids:
        print(f"⚠️ Нет связанных комментариев для проекта {project_id}.")
        return {
            "positive": 0,
            "negative": 0,
            "total": 0,
            "posts_count": posts_count,
            "top_posts": [],
            "comments_by_post": {}
        }

    # --- 4. Выбираем комментарии ---
    comments_result = await db.execute(
        select(Comment).where(Comment.id.in_(project_comment_ids))
    )
    all_comments = comments_result.scalars().all()

    print(f"🔍 Найдено {len(all_comments)} комментариев до фильтрации.")

    # --- 5. Вычисляем диапазон дат ---
    now = datetime.utcnow()
    today_start = int(datetime(now.year, now.month, now.day).timestamp())
    today_end = int(now.timestamp())
    search_start_date = today_start - (project.search_depth_days - 1) * 86400

    print(
        f"📅 Диапазон дат: от {datetime.fromtimestamp(search_start_date).date()} до {datetime.fromtimestamp(today_end).date()}")

    # --- 6. Фильтруем комментарии по дате ---
    filtered_comments = [
        c for c in all_comments
        if search_start_date <= c.date <= today_end  # теперь до "сейчас"
    ]

    print(f"🔍 Осталось {len(filtered_comments)} комментариев после фильтрации.")

    # --- 6.5. Подсчитываем посты в том же диапазоне ---
    posts_in_range = [
        p for p in all_posts
        if search_start_date <= p.date <= today_end
    ]

    print(f"📊 Найдено {len(posts_in_range)} постов в диапазоне дат.")

    # --- 7. Подсчитываем тональность ---
    total_positive = sum(1 for c in filtered_comments if c.sentiment == "positive")
    total_negative = sum(1 for c in filtered_comments if c.sentiment == "negative")
    total_comments = len(filtered_comments)

    # --- 8. Группируем комментарии по постам ---
    comments_by_post_id = {}
    for comment in filtered_comments:
        comments_by_post_id.setdefault(comment.post_id, []).append(comment)

    # --- 9. Топ-5 постов по количеству комментариев ---
    posts_with_comments = [p for p in all_posts if p.id in comments_by_post_id]
    top_posts = sorted(
        posts_with_comments,
        key=lambda p: len(comments_by_post_id[p.id]),
        reverse=True
    )[:5]

    return {
        "positive": total_positive,
        "negative": total_negative,
        "total": total_comments,
        "posts_count": len(posts_in_range),
        "top_posts": top_posts,
        "comments_by_post": comments_by_post_id
    }