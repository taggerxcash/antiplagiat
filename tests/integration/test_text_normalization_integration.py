import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from src.core.config import CoreConfig
from src.core.corpus import load_corpus
from src.core.pipeline import run_full_stage
from src.normalize import normalize_text
from src.storage.repository import SourceCorpusRepository


class TextNormalizationIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="antiplagiat_norm_integration_")
        self.corpus_dir = Path(self.temp_dir.name) / "corpus"
        self.corpus_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = Path(self.temp_dir.name) / "norm_integration.db"
        self.repo = SourceCorpusRepository(str(self.db_path))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write(self, name: str, content: str) -> Path:
        p = self.corpus_dir / name
        p.write_text(content, encoding="utf-8")
        return p

    def test_load_corpus_applies_same_normalization_rules(self) -> None:
        raw_a = "  ПрИвЕт,\u200b мир!\n\n\nСтрока  "
        raw_b = "Тестовый  документ: нор-\nмализация..."
        self._write("a.txt", raw_a)
        self._write("b.txt", raw_b)
        self._write("skip.md", "must be ignored")

        docs = load_corpus(str(self.corpus_dir), CoreConfig())
        by_path = {Path(d.path).name: d.text for d in docs}

        self.assertEqual(set(by_path), {"a.txt", "b.txt"})
        self.assertEqual(by_path["a.txt"], normalize_text(raw_a))
        self.assertEqual(by_path["b.txt"], normalize_text(raw_b))

    def test_run_full_stage_uses_same_normalizer_for_query_and_corpus(self) -> None:
        corpus_raw = (
            "Это нор-\nмализация текста, которая должна работать одинаково для запроса.\n"
            "Вторая строка внутри абзаца."
        )
        self._write("candidate.txt", corpus_raw)

        query_raw = (
            "ЭТО нормализация текста которая должна работать одинаково для запроса!!!\n"
            "вторая строка внутри абзаца"
        )
        report = run_full_stage(query_raw, str(self.corpus_dir))

        self.assertEqual(report["query_len"], len(normalize_text(query_raw)))
        self.assertGreater(report["corpus_size"], 0)
        self.assertGreater(len(report["results"]), 0)
        self.assertEqual(Path(report["results"][0]["path"]).name, "candidate.txt")

    def test_repository_stores_normalized_text_without_contract_changes(self) -> None:
        corpus_raw = "«Текст» с артефактами PDF\u200b и разрыва-\nми слов."
        expected = normalize_text(corpus_raw)
        self._write("doc.txt", corpus_raw)

        external_id = f"norm-integration-{uuid4().hex}"
        report = run_full_stage(
            query_raw_text="текст с артефактами pdf и разрывами слов",
            corpus_dir=str(self.corpus_dir),
            corpus_repo=self.repo,
            corpus_external_id=external_id,
        )
        self.assertEqual(report["corpus_size"], 1)

        corpus = self.repo.get_corpus_by_external_id(external_id)
        self.assertIsNotNone(corpus)
        assert corpus is not None

        docs = self.repo.get_documents_as_corpus_docs(corpus.id)
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0].text, expected)


if __name__ == "__main__":
    unittest.main()
