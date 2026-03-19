from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from uuid import uuid4


class SourceCorpusState(str, Enum):
    NEW = "new"
    INDEXING = "indexing"
    READY = "ready"
    FAILED = "failed"
    ARCHIVED = "archived"


class SourceDocumentState(str, Enum):
    ACTIVE = "active"
    EXCLUDED = "excluded"
    FAILED = "failed"


@dataclass(frozen=True)
class SourceCorpus:
    name: str
    root_path: str
    parameters: dict[str, Any] = field(default_factory=dict)
    is_enabled: bool = True
    state: SourceCorpusState = SourceCorpusState.NEW
    total_docs: int = 0
    indexed_docs: int = 0
    failed_docs: int = 0
    external_id: str = field(default_factory=lambda: str(uuid4()))
    last_error: str | None = None
    id: int | None = None
    created_at: str | None = None
    updated_at: str | None = None
    indexed_at: str | None = None


@dataclass(frozen=True)
class SourceCorpusDocument:
    corpus_id: int
    external_id: str
    relative_path: str
    absolute_path: str
    text_hash: str
    text_length: int
    normalized_text: str
    state: SourceDocumentState = SourceDocumentState.ACTIVE
    id: int | None = None
    created_at: str | None = None
    updated_at: str | None = None


@dataclass(frozen=True)
class SourceCorpusLink:
    corpus_id: int
    entity_type: str
    entity_external_id: str
    relation_type: str
    id: int | None = None
    created_at: str | None = None
