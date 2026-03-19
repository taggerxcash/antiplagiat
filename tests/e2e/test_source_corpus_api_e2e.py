import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient

from src.api.source_corpus_api import app, get_management_service, get_repository
from src.storage.management import SourceCorpusManagementService
from src.storage.repository import SourceCorpusRepository


class SourceCorpusApiE2ETest(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = Path(tempfile.gettempdir()) / f"antiplagiat_e2e_{uuid4().hex}.db"
        self.repo = SourceCorpusRepository(str(self.db_path))
        self.service = SourceCorpusManagementService(self.repo)

        app.dependency_overrides[get_repository] = lambda: self.repo
        app.dependency_overrides[get_management_service] = lambda: self.service
        self.client = TestClient(app)

    def tearDown(self) -> None:
        app.dependency_overrides.clear()
        try:
            self.db_path.unlink(missing_ok=True)
        except PermissionError:
            pass

    def test_full_corpus_management_lifecycle(self) -> None:
        health = self.client.get("/health")
        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.json()["status"], "ok")

        create_payload = {
            "external_id": "e2e-corpus-1",
            "name": "E2E Corpus",
            "root_path": "src/data/corpus",
            "parameters": {"language": "ru"},
            "is_enabled": True,
            "state": "new",
            "total_docs": 20,
            "indexed_docs": 0,
            "failed_docs": 0,
        }
        created = self.client.post("/api/v1/source-corpora", json=create_payload)
        self.assertEqual(created.status_code, 201, created.text)
        corpus = created.json()
        external_id = corpus["external_id"]
        self.assertEqual(corpus["name"], "E2E Corpus")
        self.assertTrue(corpus["is_enabled"])

        duplicate = self.client.post("/api/v1/source-corpora", json=create_payload)
        self.assertEqual(duplicate.status_code, 409, duplicate.text)

        listed = self.client.get("/api/v1/source-corpora?limit=10&offset=0")
        self.assertEqual(listed.status_code, 200, listed.text)
        body = listed.json()
        self.assertGreaterEqual(body["total"], 1)
        self.assertTrue(any(item["external_id"] == external_id for item in body["items"]))

        get_one = self.client.get(f"/api/v1/source-corpora/{external_id}")
        self.assertEqual(get_one.status_code, 200, get_one.text)

        update = self.client.put(
            f"/api/v1/source-corpora/{external_id}",
            json={"name": "E2E Corpus Updated", "total_docs": 21},
        )
        self.assertEqual(update.status_code, 200, update.text)
        self.assertEqual(update.json()["name"], "E2E Corpus Updated")
        self.assertEqual(update.json()["total_docs"], 21)

        params_cmd = self.client.post(
            f"/api/v1/source-corpora/{external_id}/commands/parameters",
            json={"parameters": {"min_similarity": 0.75}, "merge": True, "force": False},
        )
        self.assertEqual(params_cmd.status_code, 200, params_cmd.text)
        self.assertIn("min_similarity", params_cmd.json()["parameters"])

        state_indexing = self.client.post(
            f"/api/v1/source-corpora/{external_id}/commands/state",
            json={"target_state": "indexing", "force": False},
        )
        self.assertEqual(state_indexing.status_code, 200, state_indexing.text)
        self.assertEqual(state_indexing.json()["state"], "indexing")

        disable = self.client.post(
            f"/api/v1/source-corpora/{external_id}/commands/disable",
            json={"reason": "manual e2e disable"},
        )
        self.assertEqual(disable.status_code, 200, disable.text)
        self.assertFalse(disable.json()["is_enabled"])
        self.assertEqual(disable.json()["state"], "archived")

        invalid_state_for_disabled = self.client.post(
            f"/api/v1/source-corpora/{external_id}/commands/state",
            json={"target_state": "indexing", "force": False},
        )
        self.assertEqual(invalid_state_for_disabled.status_code, 409, invalid_state_for_disabled.text)

        enable = self.client.post(
            f"/api/v1/source-corpora/{external_id}/commands/enable",
            json={},
        )
        self.assertEqual(enable.status_code, 200, enable.text)
        self.assertTrue(enable.json()["is_enabled"])
        self.assertEqual(enable.json()["state"], "new")

        state_ready = self.client.post(
            f"/api/v1/source-corpora/{external_id}/commands/state",
            json={"target_state": "ready", "force": True},
        )
        self.assertEqual(state_ready.status_code, 200, state_ready.text)
        self.assertEqual(state_ready.json()["state"], "ready")
        self.assertIsNotNone(state_ready.json()["indexed_at"])

        deleted = self.client.delete(f"/api/v1/source-corpora/{external_id}")
        self.assertEqual(deleted.status_code, 204, deleted.text)

        missing = self.client.get(f"/api/v1/source-corpora/{external_id}")
        self.assertEqual(missing.status_code, 404, missing.text)


if __name__ == "__main__":
    unittest.main()
