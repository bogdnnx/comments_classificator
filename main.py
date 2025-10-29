# main.py
import uuid
from fastapi import FastAPI, Request, Form, Depends, BackgroundTasks, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession
from database import get_db, init_db
from models import SearchQuery
from utils import make_cache_key
from search_logic import (
    process_comments_async,
    create_initial_search_query,
    get_search_task_status,
    get_search_results
)
from project_logic import (
    create_project, get_all_projects, get_project_by_id,
    update_project, delete_project, run_project_search, get_project_stats
)

app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.on_event("startup")
async def on_startup():
    await init_db()
    print("✅ Таблицы в БД созданы (если их ещё не было)")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request, db: AsyncSession = Depends(get_db)):
    projects = await get_all_projects(db)
    return templates.TemplateResponse("index.html", {"request": request, "projects": projects})


@app.post("/search", response_class=HTMLResponse)
async def search_posts(
    request: Request,
    background_tasks: BackgroundTasks,
    query: str = Form(...),
    count: int = Form(10),
    db: AsyncSession = Depends(get_db)
):
    task_id = str(uuid.uuid4())
    await create_initial_search_query(db, query, count, task_id)
    background_tasks.add_task(process_comments_async, task_id, query, count, make_cache_key(query, count))
    return RedirectResponse(url=f"/results/{task_id}", status_code=303)


@app.get("/status/{task_id}")
async def get_status(task_id: str, db: AsyncSession = Depends(get_db)):
    return await get_search_task_status(db, task_id)


@app.get("/results/{task_id}", response_class=HTMLResponse)
async def show_results(request: Request, task_id: str, db: AsyncSession = Depends(get_db)):
    result = await get_search_results(db, task_id)
    if not result:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": "Результаты удалены или не найдены. Повторите поиск."
        })

    status = await get_search_task_status(db, task_id)
    if status["status"] == "processing":
        return templates.TemplateResponse("results_loading.html", {
            "request": request,
            "task_id": task_id,
            "query": result["query"]
        })
    if status["status"] == "error":
        return templates.TemplateResponse("error.html", {
            "request": request,
            "message": f"Ошибка обработки задачи: {status.get('error', 'unknown')}"
        })

    return templates.TemplateResponse("results.html", {
        "request": request,
        **result
    })


# --- Проекты ---
@app.post("/projects/create")
async def create_new_project(
    name: str = Form(...),
    search_depth_days: int = Form(...),
    db: AsyncSession = Depends(get_db)
):
    await create_project(db, name, search_depth_days)
    return RedirectResponse(url="/", status_code=303)


@app.post("/projects/{project_id}/search", response_class=HTMLResponse)
async def trigger_project_search(
    request: Request,
    project_id: int,
    db: AsyncSession = Depends(get_db)
):
    await run_project_search(db, project_id)  # ← запуск здесь
    return RedirectResponse(url=f"/projects/{project_id}/results?mode=full", status_code=303)

@app.post("/projects/{project_id}/quick_search", response_class=HTMLResponse)
async def trigger_project_quick_search(
    request: Request,
    project_id: int,
    db: AsyncSession = Depends(get_db)
):
    # Ничего не запускаем — просто показываем текущие данные
    return RedirectResponse(url=f"/projects/{project_id}/results?mode=quick", status_code=303)

@app.get("/projects/{project_id}/results", response_class=HTMLResponse)
async def show_project_results(
    request: Request,
    project_id: int,
    mode: str = "quick",  # по умолчанию — быстрый просмотр
    db: AsyncSession = Depends(get_db)
):
    project = await get_project_by_id(db, project_id)
    if not project:
        return templates.TemplateResponse("error.html", {"request": request, "message": "Проект не найден."})

    stats = await get_project_stats(db, project_id)

    return templates.TemplateResponse("project_results.html", {
        "request": request,
        "project": project,
        "stats": stats,
        "query": project.name,
        "mode": mode  # передаём в шаблон, чтобы показать "Быстрый" или "Полный"
    })


@app.get("/projects/{project_id}/edit", response_class=HTMLResponse)
async def get_edit_project_form(
    request: Request,
    project_id: int,
    db: AsyncSession = Depends(get_db)
):
    project = await get_project_by_id(db, project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return templates.TemplateResponse("edit_project.html", {"request": request, "project": project})


@app.post("/projects/{project_id}/update")
async def update_existing_project(
    project_id: int,
    name: str = Form(...),
    search_depth_days: int = Form(...),
    db: AsyncSession = Depends(get_db)
):
    success = await update_project(db, project_id, name, search_depth_days)
    if not success:
        raise HTTPException(status_code=404, detail="Project not found")
    return RedirectResponse(url="/", status_code=303)


@app.post("/projects/{project_id}/delete")
async def delete_existing_project(
    project_id: int,
    db: AsyncSession = Depends(get_db)
):
    await delete_project(db, project_id)
    return RedirectResponse(url="/", status_code=303)