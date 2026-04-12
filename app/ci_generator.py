from abc import ABC, abstractmethod
import logging
from pathlib import Path

from common.config import Config, get_config
from common.exceptions import CIFileExistsError, CIProfileExistsError, DBTProfilesExistsError


class AbstractCIGenerator(ABC):
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

    def __init__(self, config: Config | None = None):
        self.config = config or get_config()
        self.dbt_path = self.config.full_path_to_repo / self.config.dbt_project_name

    def _check_ci_profile(self) -> bool:
        """Checks if dbt profiles.yml exists in the project."""
        profiles_file = self.dbt_path / "profiles.yml"
        if not profiles_file.exists():
            raise DBTProfilesExistsError(f"profiles.yml not found, create it first in {self.dbt_path}")

        return "ci:" in profiles_file.read_text(encoding="utf-8")

    def create_ci_profile(self):
        """Creates CI profile for dbt project."""
        if self._check_ci_profile():
            logging.warning("CI profile already exists. Skipping creation.")
            return CIProfileExistsError

        ci_profile = (
            "\nci:\n"
            "  target: ci\n"
            "  outputs:\n"
            "    ci:\n"
            f"      type: {self.config.db_type}\n"
            f"      host: {self.config.db_dbt_host}\n"
            f"      user: {self.config.db_dbt_username}\n"
            f"      password: {self.config.db_dbt_password}\n"
            f"      port: {self.config.db_dbt_port}\n"
            f"      dbname: {self.config.db_dbt_database}\n"
            f"      schema: {self.config.db_dbt_schema}\n"
        )
        
        with open(self.dbt_path / "profiles.yml", "a", encoding="utf-8") as f:
            f.write(ci_profile)

    @abstractmethod
    def create_ci_file(self):
        """Creates CI file in the project."""
        ...

class GithubCIGenerator(AbstractCIGenerator):
    @property
    def ci_dir(self):
        return self.dbt_path.parent / ".github" / "workflows"
    
    @property
    def ci_content(self):
        return self.APP_DIR / "common" / "ci_examples" / "ci_example_github.yml"

    def create_ci_file(self):
        """Creates CI file for GitHub Actions."""
        ci_file = self.ci_dir / "ci.yml"
        ci_file.parent.mkdir(parents=True, exist_ok=True)

        if ci_file.exists() and len(ci_file.read_text()) > 0:
            logging.warning("CI file already exists. Skipping creation.")
            return CIFileExistsError

        try:
            content = self.ci_content.read_text(encoding="utf-8")

            for_replace = {
                "{service_endpoint}": self.config.service_endpoint,
                "{github_link}": self.config.github_repo_link,
                "{dbt_project_name}": self.config.dbt_project_name,
                "{base_branch}": self.config.base_branch,
                "{db_user}": self.config.db_dbt_username,
                "{db_password}": self.config.db_dbt_password,
                "{db_name}": self.config.db_dbt_database,
            }

            for key, val in for_replace.items():
                content = content.replace(key, str(val))

            with open(ci_file, "w", encoding="utf-8") as f:
                f.write(content)
        except Exception as e:
            logging.error(f"Failed to create CI file: {e}")
            return Exception(e)

        
