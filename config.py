import os
from dotenv import load_dotenv

load_dotenv()

VK_ACCESS_TOKEN = os.getenv("VK_ACCESS_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:111@localhost:5433/vk_sentiment")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CACHE_TTL = int(os.getenv("CACHE_TTL", 604800))  # 7 дней