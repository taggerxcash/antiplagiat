from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from src.utils import extract_text
from src.normalize import normalize_text
from src.core.config import CoreConfig


@dataclass(frozen=True)
class CorpusDoc:
    path: str
    text: str


def iter_corpus_files(corpus_dir: str, cfg: CoreConfig) -> Iterable[Path]:
    root = Path(corpus_dir)
    if not root.exists() or not root.is_dir():
        raise ValueError(f"Corpus dir not found or not a directory: {corpus_dir}")

    for p in root.rglob("*"):
        if p.is_file() and p.suffix.lower() in cfg.allowed_exts:
            yield p


def load_corpus(corpus_dir: str, cfg: CoreConfig) -> list[CorpusDoc]:
    docs: list[CorpusDoc] = []
    for path in iter_corpus_files(corpus_dir, cfg):
        raw = extract_text(str(path))
        norm = normalize_text(raw)
        if norm.strip():
            docs.append(CorpusDoc(path=str(path), text=norm))
    return docs
