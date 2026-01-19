from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from src.core.config import CoreConfig
from src.core.corpus import CorpusDoc


@dataclass(frozen=True)
class FastCandidate:
    path: str
    score_fast: float  # 0..1


def build_vectorizer(cfg: CoreConfig) -> TfidfVectorizer:
    return TfidfVectorizer(
        analyzer="char",
        ngram_range=(cfg.char_ngram_min, cfg.char_ngram_max),
        max_features=cfg.max_features,
        min_df=cfg.min_df,
        lowercase=False,  # мы уже нормализовали
    )


def fast_top_k(query_text: str, corpus_docs: Sequence[CorpusDoc], cfg: CoreConfig) -> List[FastCandidate]:
    if not query_text.strip():
        return []
    if not corpus_docs:
        return []

    vectorizer = build_vectorizer(cfg)

    corpus_texts = [d.text for d in corpus_docs]
    X = vectorizer.fit_transform(corpus_texts)          # (N, V)
    q = vectorizer.transform([query_text])              # (1, V)

    sims = cosine_similarity(q, X).ravel()              # (N,)
    if sims.size == 0:
        return []

    k = min(cfg.top_k, sims.size)
    top_idx = np.argpartition(-sims, kth=k - 1)[:k]     # не сортирует полностью
    top_idx = top_idx[np.argsort(-sims[top_idx])]       # сортируем top-k

    out: List[FastCandidate] = []
    for i in top_idx:
        out.append(FastCandidate(path=corpus_docs[int(i)].path, score_fast=float(sims[int(i)])))
    return out
