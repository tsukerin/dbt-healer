import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from app import context, utils


class DbtContextTests(unittest.TestCase):
    def setUp(self):
        """Store original utils config and log reader."""
        self.original_config = utils.config
        self.original_get_context_log = utils.get_context_log
        self.original_structured_sql_context = context.structured_sql_context
        utils.get_context_log = lambda: (
            'Failure in test unique_mart_customer_360_cust_id '
            '(models/mart/_mart_layer_doc.yml)\n'
            'column "cust_id" is not unique\n'
            'compiled code at target/compiled/shops_dwh/models/mart/mart_customer_360.sql'
        )

    def tearDown(self):
        """Restore original utils state."""
        utils.config = self.original_config
        utils.get_context_log = self.original_get_context_log
        context.structured_sql_context = self.original_structured_sql_context

    def _write_project(self, tmp: str) -> Path:
        repo_root = Path(tmp)
        project_path = repo_root / "shops_dwh"
        files = {
            "models/mart/mart_customer_360.sql": "select cust_id from {{ ref('stg_customers') }}",
            "models/stg/stg_customers.sql": "select cust_id from {{ ref('raw_customers') }}",
            "models/stg/raw_customers.sql": "select 1 as cust_id",
            "models/mart/customer_orders.sql": "select cust_id from {{ ref('mart_customer_360') }}",
            "models/mart/_mart_layer_doc.yml": "models:\n  - name: mart_customer_360\n    columns:\n      - name: cust_id",
            "target/compiled/shops_dwh/models/mart/mart_customer_360.sql": "select cust_id from stg_customers",
        }
        for path, content in files.items():
            file_path = project_path / path
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(content, encoding="utf-8")

        manifest_path = project_path / "target" / "manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "nodes": {
                        "model.shops_dwh.mart_customer_360": {
                            "resource_type": "model",
                            "name": "mart_customer_360",
                            "original_file_path": "models/mart/mart_customer_360.sql",
                            "depends_on": {"nodes": ["model.shops_dwh.stg_customers"]},
                        },
                        "model.shops_dwh.stg_customers": {
                            "resource_type": "model",
                            "name": "stg_customers",
                            "original_file_path": "models/stg/stg_customers.sql",
                            "depends_on": {"nodes": ["model.shops_dwh.raw_customers"]},
                        },
                        "model.shops_dwh.raw_customers": {
                            "resource_type": "model",
                            "name": "raw_customers",
                            "original_file_path": "models/stg/raw_customers.sql",
                            "depends_on": {"nodes": []},
                        },
                        "model.shops_dwh.customer_orders": {
                            "resource_type": "model",
                            "name": "customer_orders",
                            "original_file_path": "models/mart/customer_orders.sql",
                            "depends_on": {"nodes": ["model.shops_dwh.mart_customer_360"]},
                        },
                    },
                    "child_map": {
                        "model.shops_dwh.mart_customer_360": ["model.shops_dwh.customer_orders"],
                        "model.shops_dwh.stg_customers": ["model.shops_dwh.mart_customer_360"],
                        "model.shops_dwh.raw_customers": ["model.shops_dwh.stg_customers"],
                        "model.shops_dwh.customer_orders": [],
                    },
                    "macros": {},
                }
            ),
            encoding="utf-8",
        )
        utils.config = SimpleNamespace(
            dbt_project_name="shops_dwh",
            repo_root=repo_root,
            base_branch="master",
            ai_provider="Ollama",
        )
        return project_path

    def test_diagnostic_context_is_upstream_only_and_relevance_gated(self):
        """Check diagnostic context excludes downstream and keeps relevant parents."""
        with tempfile.TemporaryDirectory() as tmp:
            self._write_project(tmp)

            result = context.parse_lineage_models("models/mart/mart_customer_360.sql")
            text = "\n".join(result.values())

            self.assertIn('name="stg_customers"', text)
            self.assertIn('name="raw_customers"', text)
            self.assertNotIn("customer_orders", text)

    def test_impact_context_contains_downstream_only(self):
        """Check impact context is separated from diagnostic context."""
        with tempfile.TemporaryDirectory() as tmp:
            self._write_project(tmp)

            result = context.get_impact_context("models/mart/mart_customer_360.sql")

            self.assertIn('name="customer_orders"', result)
            self.assertNotIn('name="stg_customers"', result)

    def test_file_context_contains_primary_error_model_and_compiled_sql(self):
        """Check primary prompt context includes source and compiled SQL."""
        with tempfile.TemporaryDirectory() as tmp:
            project_path = self._write_project(tmp)

            result = context.get_file_context(str(project_path / "models/mart/mart_customer_360.sql"))

            self.assertIn("<PRIMARY_ERROR_MODEL", result)
            self.assertIn("SOURCE OF models/mart/mart_customer_360.sql", result)
            self.assertIn("<DIAGNOSTIC_CONTEXT>", result)
            self.assertIn("<SCHEMA_DEFINITION path=\"models/mart/_mart_layer_doc.yml\">", result)
            self.assertIn("<COMPILED_SQL path=", result)
            self.assertIn("Deferred until a candidate fix is generated", result)

    def test_context_shrinks_only_for_ollama_provider(self):
        """Check RAG shrinking is skipped for non-Ollama providers."""
        with tempfile.TemporaryDirectory() as tmp:
            self._write_project(tmp)
            context.structured_sql_context = lambda *args, **kwargs: "SHRUNK"

            utils.config.ai_provider = "DeepSeek API"
            result = "\n".join(context.parse_lineage_models("models/mart/mart_customer_360.sql").values())

            self.assertNotIn("SHRUNK", result)
            self.assertIn("select cust_id from {{ ref('raw_customers') }}", result)

            utils.config.ai_provider = "Ollama"
            result = "\n".join(context.parse_lineage_models("models/mart/mart_customer_360.sql").values())

            self.assertIn("SHRUNK", result)


if __name__ == "__main__":
    unittest.main()
