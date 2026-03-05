from fastapi import FastAPI, BackgroundTasks, UploadFile, File, Form, Response
import logging
import sys
from pathlib import Path
import subprocess

from app.utils import clone_repo_from_ci

PATH = str(Path(__file__).resolve().parents[1])

sys.path.append(PATH)

from common.config import get_config

config = get_config()

app = FastAPI()

def upload_failure(
    repo: str,
    commit_hash: str,
    dbt_path: str,
    log_file: UploadFile
):
    logging.info(
        "dbt failure received: repo=%s commit=%s path=%s log=%s",
        repo, commit_hash, dbt_path
    )

    clone_repo_from_ci(repo, commit_hash, dbt_path, log_file)

    subprocess.run(["python3", "run.py"], cwd=PATH, check=True)

@app.get("/health/")
def health():
    return {"status": "healer is healthy"}

@app.post("/analyze/")
def analyze(
    background_tasks: BackgroundTasks,
    repo: str = Form(...),
    commit_hash: str = Form(...),
    dbt_path: str = Form(...),
    log_file: UploadFile = File(...),
):
    try:
        background_tasks.add_task(
            upload_failure,
            repo,
            commit_hash,
            dbt_path,
            log_file
        )
        
        return {"status": "accepted"}
    except Exception as e:
        logging.error(f"Error processing failure: {e}")
        return {"status": "error", "message": str(e)}
