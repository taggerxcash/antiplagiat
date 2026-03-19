from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any, Protocol, Sequence, TYPE_CHECKING
from uuid import NAMESPACE_URL, uuid5

from src.core.config import CoreConfig
from src.storage.models import (
    SourceCorpus,
    SourceCorpusDocument,
    SourceCorpusLink,
    SourceCorpusState,
    SourceDocumentState,
)

if TYPE_CHECKING:
    from src.core.corpus import CorpusDoc


class _CorpusDocLike(Protocol):
    path: str
    text: str


class SourceCorpusRepository:
    def __init__(self, db_path: str = "src/data/antiplagiat.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.ensure_schema()

    def ensure_schema(self) -> None:
        schema_path = Path(__file__).with_name("schema.sql")
        schema_sql = schema_path.read_text(encoding="utf-8")
        with self._connect() as conn:
            conn.executescript(schema_sql)
            self._ensure_backward_compatible_schema(conn)

    @staticmethod
    def _ensure_backward_compatible_schema(conn: sqlite3.Connection) -> None:
        cols = conn.execute("PRAGMA table_info(source_corpora)").fetchall()
        names = {c["name"] for c in cols}
        if "is_enabled" not in names:
            conn.execute(
                """
                ALTER TABLE source_corpora
                ADD COLUMN is_enabled INTEGER NOT NULL DEFAULT 1 CHECK (is_enabled IN (0, 1))
                """
            )

    def upsert_from_loaded_corpus(
        self,
        corpus_dir: str,
        cfg: CoreConfig,
        docs: Sequence[_CorpusDocLike],
        external_id: str | None = None,
    ) -> SourceCorpus:
        resolved_root = str(Path(corpus_dir).resolve())
        corpus_external_id = external_id or self._build_corpus_external_id(resolved_root)
        existing = self.get_corpus_by_external_id(corpus_external_id)

        corpus = SourceCorpus(
            name=Path(resolved_root).name or "source_corpus",
            root_path=resolved_root,
            parameters=asdict(cfg),
            is_enabled=existing.is_enabled if existing else True,
            state=SourceCorpusState.INDEXING,
            total_docs=len(docs),
            indexed_docs=0,
            failed_docs=0,
            external_id=corpus_external_id,
        )
        persisted = self.upsert_corpus(corpus)
        if persisted.id is None:
            raise RuntimeError("Failed to persist source corpus record.")

        try:
            self.replace_documents(persisted.id, resolved_root, docs)
            self._update_corpus_index_stats(
                corpus_id=persisted.id,
                total_docs=len(docs),
                indexed_docs=len(docs),
                failed_docs=0,
                state=SourceCorpusState.READY,
                last_error=None,
                indexed_now=True,
            )
        except Exception as exc:
            self._update_corpus_index_stats(
                corpus_id=persisted.id,
                total_docs=len(docs),
                indexed_docs=0,
                failed_docs=len(docs),
                state=SourceCorpusState.FAILED,
                last_error=str(exc),
                indexed_now=False,
            )
            raise

        refreshed = self.get_corpus_by_external_id(corpus_external_id)
        if refreshed is None:
            raise RuntimeError("Failed to load source corpus after sync.")
        return refreshed

    def upsert_corpus(self, corpus: SourceCorpus) -> SourceCorpus:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO source_corpora (
                    external_id,
                    name,
                    root_path,
                    parameters_json,
                    is_enabled,
                    status,
                    total_docs,
                    indexed_docs,
                    failed_docs,
                    last_error,
                    indexed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(external_id) DO UPDATE SET
                    name = excluded.name,
                    root_path = excluded.root_path,
                    parameters_json = excluded.parameters_json,
                    is_enabled = excluded.is_enabled,
                    status = excluded.status,
                    total_docs = excluded.total_docs,
                    indexed_docs = excluded.indexed_docs,
                    failed_docs = excluded.failed_docs,
                    last_error = excluded.last_error,
                    indexed_at = excluded.indexed_at,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    corpus.external_id,
                    corpus.name,
                    corpus.root_path,
                    json.dumps(corpus.parameters, ensure_ascii=False, sort_keys=True),
                    int(corpus.is_enabled),
                    corpus.state.value,
                    corpus.total_docs,
                    corpus.indexed_docs,
                    corpus.failed_docs,
                    corpus.last_error,
                    corpus.indexed_at,
                ),
            )
            row = conn.execute(
                "SELECT * FROM source_corpora WHERE external_id = ?",
                (corpus.external_id,),
            ).fetchone()
        if row is None:
            raise RuntimeError("Source corpus upsert returned no rows.")
        return self._row_to_corpus(row)

    def create_corpus(self, corpus: SourceCorpus) -> SourceCorpus:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO source_corpora (
                    external_id,
                    name,
                    root_path,
                    parameters_json,
                    is_enabled,
                    status,
                    total_docs,
                    indexed_docs,
                    failed_docs,
                    last_error,
                    indexed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    corpus.external_id,
                    corpus.name,
                    corpus.root_path,
                    json.dumps(corpus.parameters, ensure_ascii=False, sort_keys=True),
                    int(corpus.is_enabled),
                    corpus.state.value,
                    corpus.total_docs,
                    corpus.indexed_docs,
                    corpus.failed_docs,
                    corpus.last_error,
                    corpus.indexed_at,
                ),
            )
            row = conn.execute(
                "SELECT * FROM source_corpora WHERE external_id = ?",
                (corpus.external_id,),
            ).fetchone()
        if row is None:
            raise RuntimeError("Source corpus create returned no rows.")
        return self._row_to_corpus(row)

    def list_corpora(self, limit: int = 100, offset: int = 0) -> list[SourceCorpus]:
        safe_limit = max(1, min(limit, 500))
        safe_offset = max(0, offset)
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM source_corpora
                ORDER BY created_at DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (safe_limit, safe_offset),
            ).fetchall()
        return [self._row_to_corpus(r) for r in rows]

    def count_corpora(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(1) AS total FROM source_corpora").fetchone()
        if row is None:
            return 0
        return int(row["total"])

    def update_corpus_fields(self, external_id: str, fields: dict[str, Any]) -> SourceCorpus | None:
        if not fields:
            return self.get_corpus_by_external_id(external_id)

        mappings = {
            "name": "name",
            "root_path": "root_path",
            "parameters": "parameters_json",
            "is_enabled": "is_enabled",
            "state": "status",
            "total_docs": "total_docs",
            "indexed_docs": "indexed_docs",
            "failed_docs": "failed_docs",
            "last_error": "last_error",
            "indexed_at": "indexed_at",
        }

        set_clauses: list[str] = []
        values: list[Any] = []
        for key, val in fields.items():
            col = mappings.get(key)
            if col is None:
                continue

            if key == "parameters":
                val = json.dumps(val or {}, ensure_ascii=False, sort_keys=True)
            elif key == "is_enabled":
                val = int(bool(val))
            elif key == "state":
                if isinstance(val, SourceCorpusState):
                    val = val.value
                else:
                    val = SourceCorpusState(str(val)).value

            set_clauses.append(f"{col} = ?")
            values.append(val)

        if not set_clauses:
            return self.get_corpus_by_external_id(external_id)

        set_clauses.append("updated_at = CURRENT_TIMESTAMP")

        with self._connect() as conn:
            cur = conn.execute(
                f"""
                UPDATE source_corpora
                SET {", ".join(set_clauses)}
                WHERE external_id = ?
                """,
                (*values, external_id),
            )
            if cur.rowcount == 0:
                return None
            row = conn.execute(
                "SELECT * FROM source_corpora WHERE external_id = ?",
                (external_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_corpus(row)

    def delete_corpus(self, external_id: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM source_corpora WHERE external_id = ?",
                (external_id,),
            )
        return cur.rowcount > 0

    def get_corpus_by_external_id(self, external_id: str) -> SourceCorpus | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM source_corpora WHERE external_id = ?",
                (external_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_corpus(row)

    def replace_documents(
        self,
        corpus_id: int,
        corpus_root: str,
        docs: Sequence[_CorpusDocLike],
    ) -> list[SourceCorpusDocument]:
        resolved_root = Path(corpus_root).resolve()
        keep_rel_paths: list[str] = []

        with self._connect() as conn:
            for doc in docs:
                abs_path = str(Path(doc.path).resolve())
                rel_path = self._to_relative_path(abs_path, resolved_root)
                keep_rel_paths.append(rel_path)

                conn.execute(
                    """
                    INSERT INTO source_corpus_documents (
                        corpus_id,
                        external_id,
                        relative_path,
                        absolute_path,
                        text_hash,
                        text_length,
                        normalized_text,
                        state
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(corpus_id, relative_path) DO UPDATE SET
                        external_id = excluded.external_id,
                        absolute_path = excluded.absolute_path,
                        text_hash = excluded.text_hash,
                        text_length = excluded.text_length,
                        normalized_text = excluded.normalized_text,
                        state = excluded.state,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        corpus_id,
                        self._build_doc_external_id(corpus_id, rel_path),
                        rel_path,
                        abs_path,
                        self._text_hash(doc.text),
                        len(doc.text),
                        doc.text,
                        SourceDocumentState.ACTIVE.value,
                    ),
                )

            if keep_rel_paths:
                placeholders = ",".join("?" for _ in keep_rel_paths)
                conn.execute(
                    f"""
                    DELETE FROM source_corpus_documents
                    WHERE corpus_id = ?
                      AND relative_path NOT IN ({placeholders})
                    """,
                    (corpus_id, *keep_rel_paths),
                )
            else:
                conn.execute(
                    "DELETE FROM source_corpus_documents WHERE corpus_id = ?",
                    (corpus_id,),
                )

            rows = conn.execute(
                """
                SELECT *
                FROM source_corpus_documents
                WHERE corpus_id = ?
                ORDER BY relative_path
                """,
                (corpus_id,),
            ).fetchall()

        return [self._row_to_source_document(r) for r in rows]

    def get_documents_as_corpus_docs(self, corpus_id: int) -> list["CorpusDoc"]:
        from src.core.corpus import CorpusDoc

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, corpus_id, absolute_path, normalized_text
                FROM source_corpus_documents
                WHERE corpus_id = ?
                ORDER BY relative_path
                """,
                (corpus_id,),
            ).fetchall()

        return [
            CorpusDoc(
                path=row["absolute_path"],
                text=row["normalized_text"],
                source_corpus_id=row["corpus_id"],
                source_doc_id=row["id"],
            )
            for row in rows
        ]

    def add_link(
        self,
        corpus_id: int,
        entity_type: str,
        entity_external_id: str,
        relation_type: str,
    ) -> SourceCorpusLink:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO source_corpus_links (
                    corpus_id,
                    entity_type,
                    entity_external_id,
                    relation_type
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(corpus_id, entity_type, entity_external_id, relation_type) DO NOTHING
                """,
                (corpus_id, entity_type, entity_external_id, relation_type),
            )
            row = conn.execute(
                """
                SELECT *
                FROM source_corpus_links
                WHERE corpus_id = ?
                  AND entity_type = ?
                  AND entity_external_id = ?
                  AND relation_type = ?
                """,
                (corpus_id, entity_type, entity_external_id, relation_type),
            ).fetchone()

        if row is None:
            raise RuntimeError("Failed to create source corpus link.")
        return self._row_to_link(row)

    def list_links(self, corpus_id: int) -> list[SourceCorpusLink]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM source_corpus_links
                WHERE corpus_id = ?
                ORDER BY created_at, id
                """,
                (corpus_id,),
            ).fetchall()
        return [self._row_to_link(r) for r in rows]

    def _update_corpus_index_stats(
        self,
        corpus_id: int,
        total_docs: int,
        indexed_docs: int,
        failed_docs: int,
        state: SourceCorpusState,
        last_error: str | None,
        indexed_now: bool,
    ) -> None:
        indexed_at_sql = "CURRENT_TIMESTAMP" if indexed_now else "indexed_at"
        with self._connect() as conn:
            conn.execute(
                f"""
                UPDATE source_corpora
                SET total_docs = ?,
                    indexed_docs = ?,
                    failed_docs = ?,
                    status = ?,
                    last_error = ?,
                    updated_at = CURRENT_TIMESTAMP,
                    indexed_at = {indexed_at_sql}
                WHERE id = ?
                """,
                (
                    total_docs,
                    indexed_docs,
                    failed_docs,
                    state.value,
                    last_error,
                    corpus_id,
                ),
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    @staticmethod
    def _build_corpus_external_id(corpus_root: str) -> str:
        return str(uuid5(NAMESPACE_URL, f"antiplagiat:source_corpus:{corpus_root.lower()}"))

    @staticmethod
    def _build_doc_external_id(corpus_id: int, relative_path: str) -> str:
        return str(uuid5(NAMESPACE_URL, f"antiplagiat:source_doc:{corpus_id}:{relative_path.lower()}"))

    @staticmethod
    def _text_hash(text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    @staticmethod
    def _to_relative_path(abs_path: str, corpus_root: Path) -> str:
        path_obj = Path(abs_path)
        try:
            return str(path_obj.relative_to(corpus_root)).replace("\\", "/")
        except ValueError:
            return path_obj.name

    @staticmethod
    def _row_to_corpus(row: sqlite3.Row) -> SourceCorpus:
        params_raw = row["parameters_json"] or "{}"
        try:
            params = json.loads(params_raw)
        except json.JSONDecodeError:
            params = {}

        return SourceCorpus(
            id=row["id"],
            external_id=row["external_id"],
            name=row["name"],
            root_path=row["root_path"],
            parameters=params,
            is_enabled=bool(row["is_enabled"]),
            state=SourceCorpusState(row["status"]),
            total_docs=row["total_docs"],
            indexed_docs=row["indexed_docs"],
            failed_docs=row["failed_docs"],
            last_error=row["last_error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            indexed_at=row["indexed_at"],
        )

    @staticmethod
    def _row_to_source_document(row: sqlite3.Row) -> SourceCorpusDocument:
        return SourceCorpusDocument(
            id=row["id"],
            corpus_id=row["corpus_id"],
            external_id=row["external_id"],
            relative_path=row["relative_path"],
            absolute_path=row["absolute_path"],
            text_hash=row["text_hash"],
            text_length=row["text_length"],
            normalized_text=row["normalized_text"],
            state=SourceDocumentState(row["state"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row_to_link(row: sqlite3.Row) -> SourceCorpusLink:
        return SourceCorpusLink(
            id=row["id"],
            corpus_id=row["corpus_id"],
            entity_type=row["entity_type"],
            entity_external_id=row["entity_external_id"],
            relation_type=row["relation_type"],
            created_at=row["created_at"],
        )
