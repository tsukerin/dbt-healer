import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from app import review, utils


class ReviewTests(unittest.TestCase):
    def setUp(self):
        """Store original utils config."""
        self.original_config = utils.config

    def tearDown(self):
        """Restore original utils config."""
        utils.config = self.original_config

    def test_review_finding_ignores_no_findings(self):
        """Check NO_FINDINGS response does not notify."""
        self.assertEqual(review.review_finding("<review>\nNO_FINDINGS\n</review>"), "")
        self.assertEqual(review.review_finding("<review>\nno_findings.\n</review>"), "")
        self.assertEqual(review.review_finding("No issues found"), "")

    def test_review_finding_extracts_finding_text(self):
        """Check finding text is extracted from review block."""
        result = review.review_finding("<review>\n<finding>\nИтог: grain changed\n</finding>\n</review>")

        self.assertEqual(result, "<finding>\nИтог: grain changed\n</finding>")

    def test_build_review_context_uses_independent_file_blocks(self):
        """Check review context separates each changed file."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            project = repo_root / "shops_dwh"
            project.mkdir()
            first_model = project / "first_model.sql"
            second_model = project / "second_model.sql"

            subprocess.run(["git", "init"], cwd=repo_root, check=True, capture_output=True)
            subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo_root, check=True)
            subprocess.run(["git", "config", "user.name", "Test"], cwd=repo_root, check=True)
            first_model.write_text("select 1 as id\n", encoding="utf-8")
            second_model.write_text("select 10 as amount\n", encoding="utf-8")
            subprocess.run(["git", "add", "."], cwd=repo_root, check=True)
            subprocess.run(["git", "commit", "-m", "base"], cwd=repo_root, check=True, capture_output=True)
            first_model.write_text("select 2 as id\n", encoding="utf-8")
            second_model.write_text("select 20 as amount\n", encoding="utf-8")
            subprocess.run(["git", "commit", "-am", "change"], cwd=repo_root, check=True, capture_output=True)

            utils.config = SimpleNamespace(
                dbt_project_name="shops_dwh",
                repo_root=repo_root,
                base_branch="main",
            )

            result = review.build_review_context()

        self.assertIn("<CHANGED_FILES>", result)
        self.assertIn('REVIEW_FILE path="shops_dwh/first_model.sql"', result)
        self.assertIn('REVIEW_FILE path="shops_dwh/second_model.sql"', result)
        self.assertIn("<FILE_DIFF>", result)
        self.assertIn("<CURRENT_FILE>", result)
        self.assertIn("-select 1 as id", result)
        self.assertIn("+select 2 as id", result)
        self.assertIn("-select 10 as amount", result)
        self.assertIn("+select 20 as amount", result)


if __name__ == "__main__":
    unittest.main()
