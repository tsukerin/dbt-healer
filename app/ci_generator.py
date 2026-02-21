from abc import ABC, abstractmethod
import logging
from pathlib import Path

from common.config import BASE_BRANCH, DBT_PROJECT_NAME, GITHUB_USERNAME, REPO_NAME, REPO_ROOT


class AbstractCIGenerator(ABC):
    DBT_PATH = REPO_ROOT / DBT_PROJECT_NAME
    APP_DIR = Path(__file__).resolve().parent.parent

    @property
    @abstractmethod
    def ci_dir(self):
        """CI directory path."""
        ...

    @property
    @abstractmethod
    def ci_content(self):
        """CI content path."""
        ...

    def __init__(self, config: dict[str, str]):
        self.config = config

    def _check_ci_profile(self) -> bool:
        """Checks if dbt profiles.yml exists in the project."""
        if not (self.DBT_PATH / 'profiles.yml').exists():
            return False
        with open(self.DBT_PATH / 'profiles.yml', 'r') as f:
            lines = f.readlines()
            return True if 'ci:' in ''.join(lines) else False

    def create_ci_profile(self):
        """Creates CI profile for dbt project."""
        if self._check_ci_profile():
            logging.info("CI profile already exists. Skipping creation.")
            return

        ci_profile = (
            "\nci:\n"
            "  target: ci\n"
            "  outputs:\n"
            "    ci:\n"
            f"      host: {self.config.get('DB_HOST', 'localhost')}\n"
            f"      user: {self.config.get('DB_USERNAME', 'dbt')}\n"
            f"      password: {self.config.get('DB_PASSWORD', 'dbt')}\n"
            f"      port: {self.config.get('DB_PORT', 5432)}\n"
            f"      dbname: {self.config.get('DB_DATABASE', 'dbt')}\n"
            f"      schema: {self.config.get('DB_SCHEMA', 'dbt')}\n"
        )
        
        with open(self.DBT_PATH / 'profiles.yml', 'a') as f:
            f.write(ci_profile)

    @abstractmethod
    def create_ci_file(self):
        """Creates CI file in the project."""
        ...

class GithubCIGenerator(AbstractCIGenerator):
    @property
    def ci_dir(self):
        return self.DBT_PATH / '.github' / 'workflows'
    
    @property
    def ci_content(self):
        return self.APP_DIR / 'common' / 'ci_examples' / 'ci_example_github.yml'

    def create_ci_file(self):
        """Creates CI file for GitHub Actions."""
        ci_file = self.ci_dir / 'ci.yml'
        ci_file.parent.mkdir(parents=True, exist_ok=True)

        if ci_file.exists() and len(ci_file.read_text()) > 0:
            logging.info("CI file already exists. Skipping creation.")
            return

        try:
            dbt_path = self.config.get('DBT_PROJECT_NAME', DBT_PROJECT_NAME) or ""
            base_branch = self.config.get('BASE_BRANCH', BASE_BRANCH) or "master"
            github_link = self.config.get('GITHUB_REPO_LINK', '')
            db_user = self.config.get('DB_USERNAME', 'dbt')
            db_password = self.config.get('DB_PASSWORD', 'dbt')
            db_name = self.config.get('DB_DATABASE', 'dbt')
            if not github_link and GITHUB_USERNAME and REPO_NAME:
                github_link = f"https://github.com/{GITHUB_USERNAME}/{REPO_NAME}.git"

            content = self.ci_content.read_text(encoding='utf-8')
            content = content.replace(
                "<analyze-endpoint>",
                    self.config.get('ANALYZE_ENDPOINT', '')
                ).replace(
                    "<github-link>",
                    github_link
                ).replace(
                    "<dbt-project-path>",
                    dbt_path
                ).replace(
                    "<base-branch>",
                    base_branch
                ).replace(
                    "<db-user>",
                    db_user
                ).replace(
                    "<db-password>",
                    db_password
                ).replace(
                    "<db-name>",
                    db_name
                )

            with open(ci_file, 'w', encoding='utf-8') as f:
                f.write(content)
        except Exception as e:
            logging.error(f"Failed to create CI file: {e}")


        
