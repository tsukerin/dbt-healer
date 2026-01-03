from pathlib import Path
import os
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]

load_dotenv(dotenv_path=REPO_ROOT / '.env')

DBT_PROJECT_NAME = os.getenv('DBT_PROJECT_NAME')
API_KEY = os.getenv('API_KEY')
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
BOT_TOKEN = os.getenv('BOT_TOKEN')
DBT_PROJECT_NAME = os.getenv('DBT_PROJECT_NAME')
GITHUB_USERNAME = os.getenv('GITHUB_USERNAME')
REPO_NAME = os.getenv('GITHUB_REPO')
BASE_BRANCH = os.getenv('BASE_BRANCH')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD') 
DB_DATABASE = os.getenv('DB_DATABASE')

LOGS_FILE = REPO_ROOT / 'logs' / 'err_hashes.txt'
DBT_LOG = REPO_ROOT / os.getenv('DBT_PROJECT_NAME') / 'logs' / 'dbt.log'