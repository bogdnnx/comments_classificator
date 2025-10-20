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

class SearchQueryDay(Base):
    __tablename__ = 'search_queries_day'

    id = Column(Integer, primary_key=True, index=True)
    query_text = Column(Text, nullable=False)
    days = Column(Integer, nullable=False)
    task_id = Column(String, unique=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    start_date = Column(DateTime, nullable=False)  # Начальная дата периода
    end_date = Column(DateTime, nullable=False)    # Конечная дата периода

class DayPost(Base):
    __tablename__ = 'day_posts'

    id = Column(Integer, primary_key=True, index=True)
    day_query_id = Column(Integer, ForeignKey('search_queries_day.id', ondelete="CASCADE"), nullable=False)
    post_id = Column(Integer, ForeignKey('posts.id', ondelete="CASCADE"), nullable=False)

