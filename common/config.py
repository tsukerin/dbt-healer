from functools import lru_cache
from pathlib import Path
import logging
import re
import os
from dotenv import set_key
from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
dotenv_path = Path(__file__).resolve().parents[1] / ".env"


def parse_github_repo_link(repo_link: str | None) -> tuple[str | None, str | None]:
    if not repo_link:
        return None, None

    match = re.match(
        r"^(?:https?://|git@)?(?:www\.)?github\.com[:/](?P<owner>[^/\s]+)/(?P<repo>[^/\s]+?)(?:\.git)?/?$",
        repo_link.strip(),
    )
    if not match:
        logging.warning("Unable to parse GITHUB_REPO_LINK: %s", repo_link)
        return None, None

    return match.group("owner"), match.group("repo")

class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=dotenv_path,
        env_file_encoding="utf-8",
        extra="ignore",
    )
    service_port: str = "8888"
    service_endpoint: str = Field(default="localhost", validation_alias="SERVICE_ENDPOINT")

    github_repo_link: str = Field(default="", validation_alias="GITHUB_REPO_LINK")
    ai_provider: str = Field(default="Ollama", validation_alias="AI_PROVIDER")
    ai_provider_type: str = Field(default="Ollama (API)", validation_alias="AI_PROVIDER_TYPE")
    ai_api_key: str = Field(default="", validation_alias="AI_API_KEY")
    ai_model: str = Field(default="", validation_alias="AI_MODEL")
    ollama_host: str = Field(default="", validation_alias="OLLAMA_HOST")
    github_token: str = ""
    telegram_bot_token: str = Field(default="", validation_alias="TELEGRAM_BOT_TOKEN")
    dbt_project_name: str = ""
    base_branch: str = "master"

    full_path_to_repo_str: str = Field(default="", validation_alias="FULL_PATH_TO_REPO")
    db_dbt_host: str = "localhost"
    db_dbt_username: str = "dbt_healer"
    db_dbt_password: str = "dbt_healer"
    db_dbt_port: int = 5432
    db_type: str = "postgres"

    notifier_db_username: str = Field(default="postgres", validation_alias="NOTIFIER_DB_USERNAME")
    notifier_db_password: str = Field(default="postgres", validation_alias="NOTIFIER_DB_PASSWORD")
    notifier_db_port: int = Field(default=5432, validation_alias="NOTIFIER_DB_PORT")

    failed_repo_path: Path | None = None

    @field_validator("github_repo_link")
    @classmethod
    def normalize_github_link(cls, v: str) -> str:
        if not v.endswith(".git"):
            v = v.rstrip("/") + ".git"
        return v
    
    @field_validator("service_endpoint")
    @classmethod
    def normalize_service_endpoint(cls, v: str) -> str:
        if v.startswith("http://"):
            v = "https://" + v.removeprefix("http://")
        if not v.endswith("/analyze/"):
            v = v.rstrip("/") + "/analyze/"
        return v

    @property
    def github_owner_repo(self) -> tuple[str | None, str | None]:
        return parse_github_repo_link(self.github_repo_link)

    @property
    def github_name(self) -> str | None:
        return self.github_owner_repo[0]

    @property
    def github_repo(self) -> str | None:
        return self.github_owner_repo[1]

    @property
    def repo_root(self) -> Path:
        return Path.home() / ".failedrepo" / (self.github_repo or "")

    @property
    def logs_file(self) -> Path:
        return self.repo_root / "logs" / "err_hashes.txt"

    @property
    def dbt_log(self) -> Path:
        if self.dbt_project_name:
            return self.repo_root / self.dbt_project_name / "logs" / "dbt.log"
        return self.repo_root / "logs" / "dbt.log"

    @property
    def uploaded_dbt_log(self) -> Path:
        return self.repo_root / "logs" / "payload_dbt.log"

    @property
    def get_profiles_path(self) -> Path:
        path = self.full_path_to_repo / self.dbt_project_name / "profiles.yml"

        return path if path.exists() else None

    @property
    def db_dbt_schema(self) -> str:
        if not self.full_path_to_repo or not self.dbt_project_name or not self.get_profiles_path:
            return None

        with open(self.get_profiles_path, mode="r") as f:
            lines = f.readlines()
            schema = re.search(pattern=r'schema:\s*(?P<schema>\S+)', string=''.join(lines))
        if not schema: 
            return
        return schema.group("schema").strip()
    
    @property
    def db_dbt_database(self) -> str:
        if not self.full_path_to_repo or not self.dbt_project_name or not self.get_profiles_path:
            return None

        with open(self.get_profiles_path, mode="r") as f:
            lines = f.readlines()
            dbname = re.search(pattern=r'dbname:\s*(?P<database>\S+)', string=''.join(lines))
        if not dbname: 
            return
        return dbname.group("database").strip()

    @property
    def bot_token(self) -> str:
        return self.telegram_bot_token
    
    @property
    def full_path_to_repo(self) -> Path:
        return Path(self.full_path_to_repo_str)
    
    @property
    def path_to_dbt_proj(self) -> Path:
        return self.full_path_to_repo / self.dbt_project_name

    def save(self, config_dict: dict[str, str | None]) -> bool:
        try:
            data = self.model_dump()
            data.update({key: val for key, val in config_dict.items() if val is not None})

            set_key(dotenv_path, "AI_PROVIDER", str(data.get("ai_provider", self.ai_provider) or "Ollama"))
            set_key(dotenv_path, "AI_PROVIDER_TYPE", str(data.get("ai_provider_type", self.ai_provider_type) or "Ollama (API)"))
            set_key(dotenv_path, "AI_API_KEY", str(data.get("ai_api_key", self.ai_api_key) or ""))
            set_key(dotenv_path, "AI_MODEL", str(data.get("ai_model", self.ai_model) or ""))
            set_key(dotenv_path, "OLLAMA_HOST", str(data.get("ollama_host", self.ollama_host) or ""))

            set_key(dotenv_path, "GITHUB_REPO_LINK", str(data.get("github_repo_link", self.github_repo_link) or ""))
            set_key(dotenv_path, "GITHUB_TOKEN", str(data.get("github_token", self.github_token) or ""))
            set_key(dotenv_path, "BASE_BRANCH", str(data.get("base_branch", self.base_branch) or "master"))

            set_key(dotenv_path, "TELEGRAM_BOT_TOKEN", str(data.get("telegram_bot_token", self.telegram_bot_token) or ""))
            set_key(dotenv_path, "BOT_TOKEN", str(data.get("telegram_bot_token", self.telegram_bot_token) or ""))

            set_key(dotenv_path, "FULL_PATH_TO_REPO", str(data.get("full_path_to_repo_str", self.full_path_to_repo_str) or ""))
            set_key(dotenv_path, "DBT_PROJECT_NAME", str(data.get("dbt_project_name", self.dbt_project_name) or ""))
            set_key(dotenv_path, "DB_DBT_HOST", str(data.get("db_dbt_host", self.db_dbt_host) or "localhost"))
            set_key(dotenv_path, "DB_DBT_USERNAME", str(data.get("db_dbt_username", self.db_dbt_username) or "dbt_healer"))
            set_key(dotenv_path, "DB_DBT_PASSWORD", str(data.get("db_dbt_password", self.db_dbt_password) or "dbt_healer"))
            set_key(dotenv_path, "DB_DBT_DATABASE", str(data.get("db_dbt_database", self.db_dbt_database) or "dbt_healer"))
            set_key(dotenv_path, "DB_DBT_PORT", str(data.get("db_dbt_port", self.db_dbt_port) or 5432))
            set_key(dotenv_path, "DB_DBT_SCHEMA", str(data.get("db_dbt_schema", self.db_dbt_schema) or "dbt_healer"))
            set_key(dotenv_path, "DB_TYPE", str(data.get("db_type", self.db_type) or "postgres"))

            set_key(dotenv_path, "SERVICE_ENDPOINT", str(data.get("service_endpoint", self.service_endpoint) or "localhost"))
            set_key(dotenv_path, "SERVICE_PORT", str(data.get("service_port", self.service_port) or "8888"))

            set_key(dotenv_path, "NOTIFIER_DB_USERNAME", "postgres")
            set_key(dotenv_path, "NOTIFIER_DB_PASSWORD", "postgres")
            set_key(dotenv_path, "NOTIFIER_DB_DATABASE", "postgres")
            set_key(dotenv_path, "NOTIFIER_DB_PORT", "5432")

            get_config.cache_clear()
        except Exception as exc:
            logging.error("Error saving config: %s", exc)
            return False

        return True

    def __str__(self) -> str:
        return (
            f"SERVICE_ENDPOINT: {self.service_endpoint}\n"
            f"GITHUB_REPO_LINK: {self.github_repo_link}\n"
            f"AI_PROVIDER: {self.ai_provider}\n"
            f"AI_PROVIDER_TYPE: {self.ai_provider_type}\n"
            f"AI_API_KEY: {'***' if self.ai_api_key else None}\n"
            f"OLLAMA_HOST: {self.ollama_host or None}\n"
            f"GITHUB_TOKEN: {'***' if self.github_token else None}\n"
            f"TELEGRAM_BOT_TOKEN: {'***' if self.telegram_bot_token else None}\n"
            f"DBT_PROJECT_NAME: {self.dbt_project_name}\n"
            f"BASE_BRANCH: {self.base_branch}\n"
            f"DB_DBT_USERNAME: {self.db_dbt_username}\n"
            f"DB_DBT_PASSWORD: {'***' if self.db_dbt_password else None}\n"
            f"DB_DBT_DATABASE: {self.db_dbt_database}\n"
            f"DB_DBT_PORT: {self.db_dbt_port}"
        )


@lru_cache(maxsize=1)
def get_config() -> Config:
    return Config()
