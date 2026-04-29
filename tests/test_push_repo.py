import unittest
from types import SimpleNamespace

from app import push_repo


class PushRepoTests(unittest.TestCase):
    def setUp(self):
        self.original_config = push_repo.config
        push_repo.config = SimpleNamespace(dbt_project_name="shops_dwh")

    def tearDown(self):
        push_repo.config = self.original_config

    def test_extract_solution_parts_skips_malformed_sections(self):
        solution = (
            "bad text\n----\n"
            "<solution>\nselect 1\n</solution>\n"
            "<file>\nmodels/core/customers.sql\n</file>"
        )

        self.assertEqual(
            push_repo.extract_solution_parts(solution),
            [("\nselect 1\n", "\nmodels/core/customers.sql\n")],
        )

    def test_build_repo_file_path_adds_dbt_project_name(self):
        self.assertEqual(
            push_repo.build_repo_file_path("models/core/customers.sql"),
            "shops_dwh/models/core/customers.sql",
        )

    def test_build_repo_file_path_keeps_project_prefixed_path(self):
        self.assertEqual(
            push_repo.build_repo_file_path("shops_dwh/models/core/customers.sql"),
            "shops_dwh/models/core/customers.sql",
        )


if __name__ == "__main__":
    unittest.main()

