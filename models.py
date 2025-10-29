# models.py
from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, Float, CheckConstraint, func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class SearchQuery(Base):
    __tablename__ = 'search_queries'

    id = Column(Integer, primary_key=True, index=True)
    query_text = Column(Text, nullable=False)
    count = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    expires_at = Column(DateTime, nullable=False)
    task_id = Column(String, unique=True, index=True)

class Post(Base):
    __tablename__ = 'posts'

    id = Column(Integer, primary_key=True, index=True)
    vk_post_id = Column(Integer, nullable=False)
    owner_id = Column(Integer, nullable=False)
    text = Column(Text)
    date = Column(Integer)
    url = Column(String)
    search_query_id = Column(Integer, ForeignKey('search_queries.id', ondelete="CASCADE"))

class Comment(Base):
    __tablename__ = 'comments'

    id = Column(Integer, primary_key=True, index=True)
    vk_comment_id = Column(Integer, nullable=False)
    post_id = Column(Integer, ForeignKey('posts.id', ondelete="CASCADE"), nullable=False)
    from_id = Column(Integer)
    text = Column(Text, nullable=False)
    sentiment = Column(String, CheckConstraint("sentiment IN ('positive', 'negative')"))
    sentiment_confidence = Column(Float)
    date = Column(Integer)
    classified_at = Column(DateTime(timezone=True), server_default=func.now())

# --- Таблица Project ---
class Project(Base):
    __tablename__ = 'projects'

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False) # Тема
    search_depth_days = Column(Integer, nullable=False) # Глубина поиска в днях
    # Используем offset-naive datetime, как в остальных таблицах
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())

class ProjectSearchQuery(Base):
    __tablename__ = 'project_search_queries'

    id = Column(Integer, primary_key=True, index=True)
    project_id = Column(Integer, ForeignKey('projects.id', ondelete="CASCADE"), nullable=False)
    search_query_id = Column(Integer, ForeignKey('search_queries.id', ondelete="CASCADE"), nullable=False)

class ProjectComment(Base):
    __tablename__ = 'project_comments'

    id = Column(Integer, primary_key=True, index=True)
    project_id = Column(Integer, ForeignKey('projects.id', ondelete="CASCADE"), nullable=False)
    comment_id = Column(Integer, ForeignKey('comments.id', ondelete="CASCADE"), nullable=False)
    # Можно добавить дополнительные поля, если нужно, например, дата добавления в проект
    added_at = Column(DateTime, server_default=func.now())