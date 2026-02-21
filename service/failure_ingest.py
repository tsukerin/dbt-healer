from fastapi import FastAPI, BackgroundTasks, UploadFile, File, Form
import logging
import sys
from pathlib import Path
import subprocess

PATH = str(Path(__file__).resolve().parents[1])

sys.path.append(PATH)

from common.config import DBT_LOG, REPO_ROOT, DBT_PROJECT_NAME

app = FastAPI()

def upload_failure(
    repo: str,
    commit_hash: str,
    dbt_path: str,
):
    logging.info(
        "dbt failure received: repo=%s commit=%s path=%s log=%s",
        repo, commit_hash, dbt_path
    )

    subprocess.run(
        ["python", "run.py", repo, commit_hash, dbt_path],
        cwd=PATH,
        check=True,
    )


@app.post("/analyze/")
def analyze(
    background_tasks: BackgroundTasks,
    repo: str = Form(...),
    commit_hash: str = Form(...),
    dbt_path: str = Form(...),
    log_file: UploadFile = File(...),
):
    DBT_LOG.parent.mkdir(parents=True, exist_ok=True)

    with open(DBT_LOG, "wb") as f:
        f.write(log_file.file.read())

    background_tasks.add_task(
        upload_failure,
        repo,
        commit_hash,
        dbt_path,
    )

    return {"status": "accepted"}