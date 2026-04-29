import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from app.ci_generator import GithubCIGenerator
from common.exceptions import DBTProfilesExistsError


class CIGeneratorTests(unittest.TestCase):
    def test_create_ci_profile_requires_existing_profiles_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = SimpleNamespace(
                full_path_to_repo=Path(tmp),
                dbt_project_name="analytics",
                db_type="postgres",
                db_dbt_host="localhost",
                db_dbt_username="dbt",
                db_dbt_password="dbt",
                db_dbt_port=5432,
                db_dbt_database="analytics",
                db_dbt_schema="public",
            )
            (Path(tmp) / "analytics").mkdir()
            generator = GithubCIGenerator(config=config)

            with self.assertRaises(DBTProfilesExistsError):
                generator.create_ci_profile()

    def test_create_ci_profile_appends_ci_target_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_path = Path(tmp) / "analytics"
            project_path.mkdir()
            profiles = project_path / "profiles.yml"
            profiles.write_text("analytics:\n  target: dev\n", encoding="utf-8")

            config = SimpleNamespace(
                full_path_to_repo=Path(tmp),
                dbt_project_name="analytics",
                db_type="postgres",
                db_dbt_host="localhost",
                db_dbt_username="dbt",
                db_dbt_password="dbt",
                db_dbt_port=5432,
                db_dbt_database="analytics",
                db_dbt_schema="public",
            )
            generator = GithubCIGenerator(config=config)

            self.assertEqual(generator.create_ci_profile(), "created")
            self.assertIn("ci:", profiles.read_text(encoding="utf-8"))
            self.assertEqual(generator.create_ci_profile(), "exists")


if __name__ == "__main__":
    unittest.main()

