from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from src.core.config import CoreConfig
from src.core.exact_shingles import build_shingles, ShingleSet
from src.core.exact_match import _merge_spans  # используем уже написанную функцию


@dataclass(frozen=True)
class Block:
    start_word: int
    end_word: int
    words: int
    text: str
    context: str


@dataclass(frozen=True)
class BlockPair:
    query: Block
    source: Block


def _span_to_text(words: Sequence[str], start: int, end: int) -> str:
    return " ".join(words[start:end])


def _span_with_context(words: Sequence[str], start: int, end: int, ctx: int = 20) -> str:
    a = max(0, start - ctx)
    b = min(len(words), end + ctx)
    return " ".join(words[a:b])


def _blocks_from_spans(words: list[str], spans: list[tuple[int, int]], cfg: CoreConfig) -> list[Block]:
    merged = _merge_spans(spans, gap=cfg.merge_gap_words)
    blocks: list[Block] = []
    for a, b in merged:
        wcount = b - a
        if wcount < cfg.min_block_words:
            continue
        blocks.append(
            Block(
                start_word=a,
                end_word=b,
                words=wcount,
                text=_span_to_text(words, a, b),
                context=_span_with_context(words, a, b, ctx=20),
            )
        )
    blocks.sort(key=lambda bl: bl.words, reverse=True)
    return blocks[: cfg.max_blocks_per_doc]


def build_block_pairs(query_text: str, source_text: str, cfg: CoreConfig) -> list[BlockPair]:
    q = build_shingles(query_text, cfg.shingle_k)
    s = build_shingles(source_text, cfg.shingle_k)

    if not q.hashes or not s.hashes:
        return []

    q_set = set(q.hashes)
    s_set = set(s.hashes)
    inter = q_set.intersection(s_set)
    if not inter:
        return []

    # spans в query
    q_spans: list[tuple[int, int]] = []
    for (h, (a, b)) in zip(q.hashes, q.ranges):
        if h in inter:
            q_spans.append((a, b))

    # spans в source
    s_spans: list[tuple[int, int]] = []
    for (h, (a, b)) in zip(s.hashes, s.ranges):
        if h in inter:
            s_spans.append((a, b))

    q_blocks = _blocks_from_spans(q.words, q_spans, cfg)
    s_blocks = _blocks_from_spans(s.words, s_spans, cfg)

    # MVP-спаривание: по порядку (самые длинные к самым длинным)
    pairs: list[BlockPair] = []
    for qb, sb in zip(q_blocks, s_blocks):
        pairs.append(BlockPair(query=qb, source=sb))

    return pairs
