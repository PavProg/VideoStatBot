from pydantic_settings import SettingsConfigDict, BaseSettings
from pathlib import Path
import sys

possible_paths = [
    Path(__file__).parent.parent.parent / '.env',  # корень проекта
    Path(__file__).parent / '.env',                # рядом с config.py
    Path.cwd() / '.env',                           # текущая директория
]

env_file = None
for path in possible_paths:
    if path.exists():
        env_file = path
        break

if env_file is None:
    print(f"ERROR: .env file not found! Searched in:", file=sys.stderr)
    for path in possible_paths:
        print(f"  - {path}", file=sys.stderr)
    sys.exit(1)

BASE_DIR = Path(__file__).resolve().parent.parent.parent

class Settings(BaseSettings):
    DB_HOST: str
    DB_PORT: int
    DB_USER: str
    DB_PASS: str
    DB_NAME: str
    TOKEN: str
    LOG_LEVEL: str
    LOG_FILE: str

    YC_API_KEY: str
    YC_MODELS: str
    YC_TEMPERATURE: float
    YC_MAX_TOKENS: int
    YC_FOLDER_ID: str

    # DB
    @property
    def DATABASE_URL_asyncpg(self):
        "postgresql+asyncpg://postgres:postgres@localhost:5432/dab"
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    # Token
    @property
    def RE_TOKEN(self):
        return self.TOKEN

    # logging
    @property
    def logger_file(self):
        return self.LOG_FILE

    @property
    def logger_level(self):
        return self.LOG_LEVEL

    # YC API
    @property
    def RE_YC_KEY(self):
        return self.YC_API_KEY

    @property
    def RE_YC_MODELS(self):
        return self.YC_MODELS

    @property
    def RE_YC_TEMPERATURE(self):
        return self.YC_TEMPERATURE

    @property
    def RE_YC_MAX_TOKENS(self):
        return self.YC_MAX_TOKENS

    @property
    def RE_YC_FOLDER_ID(self):
        return self.YC_FOLDER_ID

    model_config = SettingsConfigDict(env_file=BASE_DIR / '.env')

settings = Settings()