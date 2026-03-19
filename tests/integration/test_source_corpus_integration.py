import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from src.core.pipeline import run_full_stage
from src.storage.management import (
    ChangeSourceCorpusStateCommand,
    DisableSourceCorpusCommand,
    EnableSourceCorpusCommand,
    SourceCorpusManagementService,
    UpdateSourceCorpusParametersCommand,
)
from src.storage.models import SourceCorpusState
from src.storage.repository import SourceCorpusRepository
from src.utils import extract_text


class SourceCorpusIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = Path(tempfile.gettempdir()) / f"antiplagiat_integration_{uuid4().hex}.db"
        self.repo = SourceCorpusRepository(str(self.db_path))
        self.service = SourceCorpusManagementService(self.repo)

    def tearDown(self) -> None:
        try:
            self.db_path.unlink(missing_ok=True)
        except PermissionError:
            pass

    def test_pipeline_and_management_work_together(self) -> None:
        query_raw_text = extract_text("src/data/tests/case_1_copy.txt")
        external_id = f"integration-corpus-{uuid4().hex}"

        report = run_full_stage(
            query_raw_text=query_raw_text,
            corpus_dir="src/data/corpus",
            corpus_repo=self.repo,
            corpus_external_id=external_id,
        )

        self.assertGreater(report["corpus_size"], 0)
        self.assertIsInstance(report["results"], list)
        self.assertGreater(len(report["results"]), 0)

        corpus = self.repo.get_corpus_by_external_id(external_id)
        self.assertIsNotNone(corpus)
        assert corpus is not None
        self.assertEqual(corpus.state, SourceCorpusState.READY)
        self.assertTrue(corpus.is_enabled)
        self.assertEqual(corpus.total_docs, report["corpus_size"])
        self.assertEqual(corpus.indexed_docs, report["corpus_size"])
        self.assertEqual(corpus.failed_docs, 0)
        self.assertIsNotNone(corpus.indexed_at)

        docs = self.repo.get_documents_as_corpus_docs(corpus.id)
        self.assertEqual(len(docs), report["corpus_size"])
        self.assertTrue(all(d.source_corpus_id == corpus.id for d in docs))
        self.assertTrue(all(d.source_doc_id is not None for d in docs))

        updated_params = self.service.update_parameters(
            UpdateSourceCorpusParametersCommand(
                external_id=external_id,
                parameters={"language": "ru", "top_k_override": 5},
                merge=True,
            )
        )
        self.assertEqual(updated_params.parameters.get("language"), "ru")
        self.assertEqual(updated_params.parameters.get("top_k_override"), 5)

        disabled = self.service.disable(
            DisableSourceCorpusCommand(external_id=external_id, reason="maintenance")
        )
        self.assertFalse(disabled.is_enabled)
        self.assertEqual(disabled.state, SourceCorpusState.ARCHIVED)

        enabled = self.service.enable(EnableSourceCorpusCommand(external_id=external_id))
        self.assertTrue(enabled.is_enabled)
        self.assertEqual(enabled.state, SourceCorpusState.NEW)

        indexing = self.service.change_state(
            ChangeSourceCorpusStateCommand(
                external_id=external_id,
                target_state=SourceCorpusState.INDEXING,
            )
        )
        self.assertEqual(indexing.state, SourceCorpusState.INDEXING)
        self.assertIsNone(indexing.indexed_at)

        ready = self.service.change_state(
            ChangeSourceCorpusStateCommand(
                external_id=external_id,
                target_state=SourceCorpusState.READY,
            )
        )
        self.assertEqual(ready.state, SourceCorpusState.READY)
        self.assertIsNotNone(ready.indexed_at)


if __name__ == "__main__":
    unittest.main()
