import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from src.core.pipeline import run_full_stage
from src.normalize import normalize_text
from src.storage.models import SourceCorpusState
from src.storage.repository import SourceCorpusRepository


class TextNormalizationE2ETest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="antiplagiat_norm_e2e_")
        self.corpus_dir = Path(self.temp_dir.name) / "corpus"
        self.corpus_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = Path(self.temp_dir.name) / "norm_e2e.db"
        self.repo = SourceCorpusRepository(str(self.db_path))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write(self, name: str, text: str) -> Path:
        p = self.corpus_dir / name
        p.write_text(text, encoding="utf-8")
        return p

    def test_end_to_end_normalization_flow_is_consistent(self) -> None:
        matching_raw = (
            "«Сложный» PDF-текст с артефактами:\u200b нор-\nмализация,\n"
            "повторяющиеся   пробелы и  “кавычки”.\n\n\nНовый абзац."
        )
        other_raw = "Совсем другой документ без совпадений."
        self._write("matching.txt", matching_raw)
        self._write("other.txt", other_raw)

        query_raw = (
            "Сложный pdf текст с артефактами нормализация повторяющиеся пробелы "
            "и кавычки.\nНовый абзац!"
        )
        external_id = f"norm-e2e-{uuid4().hex}"

        report = run_full_stage(
            query_raw_text=query_raw,
            corpus_dir=str(self.corpus_dir),
            corpus_repo=self.repo,
            corpus_external_id=external_id,
        )

        self.assertEqual(report["query_len"], len(normalize_text(query_raw)))
        self.assertEqual(report["corpus_size"], 2)
        self.assertGreater(len(report["results"]), 0)
        self.assertEqual(Path(report["results"][0]["path"]).name, "matching.txt")

        corpus = self.repo.get_corpus_by_external_id(external_id)
        self.assertIsNotNone(corpus)
        assert corpus is not None
        self.assertEqual(corpus.state, SourceCorpusState.READY)
        self.assertEqual(corpus.total_docs, 2)
        self.assertEqual(corpus.indexed_docs, 2)
        self.assertEqual(corpus.failed_docs, 0)

        docs = self.repo.get_documents_as_corpus_docs(corpus.id)
        normalized_by_name = {Path(doc.path).name: doc.text for doc in docs}

        self.assertEqual(normalized_by_name["matching.txt"], normalize_text(matching_raw))
        self.assertEqual(normalized_by_name["other.txt"], normalize_text(other_raw))
        self.assertNotIn("\u200b", normalized_by_name["matching.txt"])
        self.assertNotIn("«", normalized_by_name["matching.txt"])
        self.assertNotIn("»", normalized_by_name["matching.txt"])
        self.assertNotIn("—", normalized_by_name["matching.txt"])


if __name__ == "__main__":
    unittest.main()
