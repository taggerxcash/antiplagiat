from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Sequence

from src.core.config import CoreConfig
from src.core.exact_shingles import ShingleSet, build_shingles


@dataclass(frozen=True)
class MatchBlock:
    start_word: int
    end_word: int
    words: int
    text: str
    context: str


@dataclass(frozen=True)
class ExactResult:
    score_exact: float              # 0..1
    matched_shingles: int
    total_query_shingles: int
    blocks: list[MatchBlock]


def _merge_spans(spans: list[tuple[int, int]], gap: int) -> list[tuple[int, int]]:
    """
    spans: list of (start_word, end_word) in candidate words.
    gap: allow merging if next_start <= current_end + gap.
    """
    if not spans:
        return []
    spans.sort(key=lambda x: (x[0], x[1]))
    merged = [spans[0]]
    for s, e in spans[1:]:
        ps, pe = merged[-1]
        if s <= pe + gap:
            merged[-1] = (ps, max(pe, e))
        else:
            merged.append((s, e))
    return merged


def _span_to_text(words: Sequence[str], start: int, end: int) -> str:
    return " ".join(words[start:end])


def _span_with_context(words: Sequence[str], start: int, end: int, ctx: int = 20) -> str:
    a = max(0, start - ctx)
    b = min(len(words), end + ctx)
    return " ".join(words[a:b])


def exact_compare(query_text: str, cand_text: str, cfg: CoreConfig) -> ExactResult:
    q = build_shingles(query_text, cfg.shingle_k)
    c = build_shingles(cand_text, cfg.shingle_k)

    if not q.hashes or not c.hashes:
        return ExactResult(
            score_exact=0.0,
            matched_shingles=0,
            total_query_shingles=len(q.hashes),
            blocks=[],
        )

    q_set = set(q.hashes)
    c_set = set(c.hashes)
    
    inter = q_set.intersection(c_set)
    union = q_set.union(c_set)
    
    # Жаккар: доля общих шинглов среди всех уникальных шинглов
    score_exact = len(inter) / max(1, len(union))
    
    # Для блоков нам нужны диапазоны кандидата, где шингл входит в пересечение
    spans: list[tuple[int, int]] = []
    for (h, (a, b)) in zip(c.hashes, c.ranges):
        if h in inter:
            spans.append((a, b))
    
    # matched_shingles — число УНИКАЛЬНЫХ совпавших шинглов
    matched = len(inter)


    # Склеиваем перекрывающиеся/близкие диапазоны
    merged = _merge_spans(spans, gap=cfg.merge_gap_words)

    # Конвертим в блоки + фильтруем слишком короткие
    blocks: list[MatchBlock] = []
    for a, b in merged:
        wcount = b - a
        if wcount < cfg.min_block_words:
            continue
        blocks.append(
            MatchBlock(
                start_word=a,
                end_word=b,
                words=wcount,
                text=_span_to_text(c.words, a, b),
                context=_span_with_context(c.words, a, b, ctx=20),
            )
        )


    # Сортируем блоки по длине (самые большие — важнее) и режем до max_blocks_per_doc
    blocks.sort(key=lambda bl: bl.words, reverse=True)
    blocks = blocks[: cfg.max_blocks_per_doc]

    return ExactResult(
        score_exact=float(score_exact),
        matched_shingles=int(matched),
        total_query_shingles=int(len(q.hashes)),
        blocks=blocks,
    )
