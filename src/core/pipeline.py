from __future__ import annotations

from dataclasses import asdict
from typing import Any

from src.normalize import normalize_text
from src.core.config import CoreConfig
from src.core.corpus import load_corpus
from src.core.fast_tfidf import fast_top_k
from src.core.exact_match import exact_compare
from src.core.exact_pairs import build_block_pairs


def run_fast_stage(query_raw_text: str, corpus_dir: str, cfg: CoreConfig | None = None) -> dict[str, Any]:
    """
    MVP-выход для UI: список top-k кандидатов и метаданные.
    """
    cfg = cfg or CoreConfig()

    query_text = normalize_text(query_raw_text)
    corpus_docs = load_corpus(corpus_dir, cfg)

    candidates = fast_top_k(query_text, corpus_docs, cfg)

    return {
        "config": asdict(cfg),
        "query_len": len(query_text),
        "corpus_size": len(corpus_docs),
        "candidates": [asdict(c) for c in candidates],
    }

def run_full_stage(query_raw_text: str, corpus_dir: str, cfg: CoreConfig | None = None) -> dict:
    """
    Полный MVP: fast top-k -> exact compare -> агрегированный отчёт.
    """
    cfg = cfg or CoreConfig()

    query_text = normalize_text(query_raw_text)
    corpus_docs = load_corpus(corpus_dir, cfg)

    candidates_fast = fast_top_k(query_text, corpus_docs, cfg)

    # Быстрое сопоставление path -> текст кандидата (чтобы не перечитывать файлы)
    text_by_path = {d.path: d.text for d in corpus_docs}

    results = []
    for c in candidates_fast:
        cand_text = text_by_path.get(c.path, "")
        ex = exact_compare(query_text, cand_text, cfg)
        pairs = build_block_pairs(query_text, cand_text, cfg)

        score_final = cfg.weight_fast * c.score_fast + cfg.weight_exact * ex.score_exact

        results.append({
            "path": c.path,
            "score_fast": float(c.score_fast),
            "score_exact": float(ex.score_exact),
            "score_final": float(score_final),
            "matched_shingles": ex.matched_shingles,
            "total_query_shingles": ex.total_query_shingles,
            "blocks": [asdict(b) for b in ex.blocks],
            "pairs": [
                {
                    "query": {
                        "start_word": p.query.start_word,
                        "end_word": p.query.end_word,
                        "words": p.query.words,
                        "text": p.query.text,
                        "context": p.query.context,
                    },
                    "source": {
                        "start_word": p.source.start_word,
                        "end_word": p.source.end_word,
                        "words": p.source.words,
                        "text": p.source.text,
                        "context": p.source.context,
                    },
                }
                for p in pairs
            ],
        })

    # сортируем по итоговому скору
    results.sort(key=lambda r: r["score_final"], reverse=True)

    return {
        "query_len": len(query_text),
        "corpus_size": len(corpus_docs),
        "config": asdict(cfg),
        "results": results,
    }