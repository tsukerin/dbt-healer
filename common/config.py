from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / '.env')

GOOGLEAI_API_KEY = os.getenv('GOOGLEAI_API_KEY')
OLLAMA_API_KEY = os.getenv('OLLAMA_API_KEY')

DBT_PROJECT_NAME = os.getenv('DBT_PROJECT_NAME')
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
BOT_TOKEN = os.getenv('BOT_TOKEN')
GITHUB_USERNAME = os.getenv('GITHUB_USERNAME')
REPO_NAME = os.getenv('GITHUB_REPO')
BASE_BRANCH = os.getenv('BASE_BRANCH')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD') 
DB_DATABASE = os.getenv('DB_DATABASE')

REPO_ROOT = Path.home() / ".failedrepo" / REPO_NAME

LOGS_FILE = REPO_ROOT / 'logs' / 'err_hashes.txt'
DBT_LOG = REPO_ROOT / DBT_PROJECT_NAME / 'logs' / 'dbt.log'