import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from app import utils


class DbtLogParsingTests(unittest.TestCase):
    def setUp(self):
        self.original_config = utils.config

    def tearDown(self):
        utils.config = self.original_config

    def test_extracts_explicit_model_path_from_dbt_error(self):
        log = "Database Error in model customers (models/core/customers.sql)"

        self.assertEqual(
            utils.get_error_files_from_dbt_log(log),
            ["models/core/customers.sql"],
        )

    def test_extracts_macro_path_from_dbt_error(self):
        log = "Compilation Error in macro cents_to_dollars (macros/money.sql)"

        self.assertEqual(
            utils.get_error_files_from_dbt_log(log),
            ["macros/money.sql"],
        )

    def test_resolves_model_name_from_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            project_path = repo_root / "jaffle_shop"
            manifest_path = project_path / "target" / "manifest.json"
            manifest_path.parent.mkdir(parents=True)
            manifest_path.write_text(
                json.dumps(
                    {
                        "nodes": {
                            "model.jaffle_shop.customers": {
                                "resource_type": "model",
                                "name": "customers",
                                "alias": "customers",
                                "original_file_path": "models/core/customers.sql",
                            }
                        },
                        "macros": {},
                    }
                ),
                encoding="utf-8",
            )
            utils.config = SimpleNamespace(dbt_project_name="jaffle_shop", repo_root=repo_root)

            self.assertEqual(
                utils.get_error_files_from_dbt_log("Database Error in model customers"),
                ["models/core/customers.sql"],
            )

    def test_test_failure_resolves_tested_model_from_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            project_path = repo_root / "shops_dwh"
            manifest_path = project_path / "target" / "manifest.json"
            manifest_path.parent.mkdir(parents=True)
            manifest_path.write_text(
                json.dumps(
                    {
                        "nodes": {
                            "model.shops_dwh.mart_customer_360": {
                                "resource_type": "model",
                                "name": "mart_customer_360",
                                "alias": "mart_customer_360",
                                "original_file_path": "models/mart/mart_customer_360.sql",
                            },
                            "test.shops_dwh.unique_mart_customer_360_cust_id.abc123": {
                                "resource_type": "test",
                                "name": "unique_mart_customer_360_cust_id",
                                "original_file_path": "models/mart/_mart_layer_doc.yml",
                                "depends_on": {
                                    "nodes": ["model.shops_dwh.mart_customer_360"]
                                },
                            },
                        },
                        "macros": {},
                    }
                ),
                encoding="utf-8",
            )
            utils.config = SimpleNamespace(dbt_project_name="shops_dwh", repo_root=repo_root)

            log = "Failure in test unique_mart_customer_360_cust_id (models/mart/_mart_layer_doc.yml)"

            self.assertEqual(
                utils.get_error_files_from_dbt_log(log),
                ["models/mart/mart_customer_360.sql"],
            )

    def test_test_failure_infers_model_from_test_name_without_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            project_path = repo_root / "shops_dwh"
            model_path = project_path / "models" / "mart" / "mart_customer_360.sql"
            model_path.parent.mkdir(parents=True)
            model_path.write_text("select 1 as cust_id", encoding="utf-8")
            utils.config = SimpleNamespace(dbt_project_name="shops_dwh", repo_root=repo_root)

            log = "Failure in test unique_mart_customer_360_cust_id (models/mart/_mart_layer_doc.yml)"

            self.assertEqual(
                utils.get_error_files_from_dbt_log(log),
                ["models/mart/mart_customer_360.sql"],
            )

    def test_unresolved_test_failure_does_not_fall_back_to_yaml_doc(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            project_path = repo_root / "shops_dwh"
            project_path.mkdir(parents=True)
            utils.config = SimpleNamespace(dbt_project_name="shops_dwh", repo_root=repo_root)

            log = "Failure in test unique_mart_customer_360_cust_id (models/mart/_mart_layer_doc.yml)"

            self.assertEqual(utils.get_error_files_from_dbt_log(log), [])

    def test_clean_log_removes_ansi_escape_sequences(self):
        log = "\x1b[31mDatabase Error in model customers (models/core/customers.sql)\x1b[0m"

        self.assertEqual(
            utils.get_error_files_from_dbt_log(log),
            ["models/core/customers.sql"],
        )


if __name__ == "__main__":
    unittest.main()
