from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel, field_validator
import logging

class Item(BaseModel):
    repo: str
    commit_hash: str
    dbt_path: str

    @field_validator("repo")
    def repo_must_be_git(cls, v) -> str:
        if not v.endswith(".git"):
            raise ValueError("repo must be a git repository")
        return v

app = FastAPI()

def analyze_failure(repo: str, commit_hash: str, dbt_path: str):
    logging.info(
        "dbt failure received: repo=%s commit=%s path=%s",
        repo, commit_hash, dbt_path
    )
    # trigger

@app.post("/analyze/")
def create_item(item: Item, background_tasks: BackgroundTasks):
    background_tasks.add_task(
        analyze_failure, 
        item.repo, 
        item.commit_hash, 
        item.dbt_path
    )
    return {"message": "Error has confirmed"}