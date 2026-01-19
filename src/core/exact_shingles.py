from __future__ import annotations

from dataclasses import dataclass
import re
import hashlib


_WORD_RE = re.compile(r"[a-zа-яё0-9]+", re.IGNORECASE)


def tokenize_words(text: str) -> list[str]:
    return _WORD_RE.findall(text)


def shingles(words: list[str], k: int) -> list[tuple[int, int]]:
    if k <= 0:
        raise ValueError("k must be > 0")
    if len(words) < k:
        return []
    return [(i, i + k) for i in range(0, len(words) - k + 1)]


def shingle_hash(words: list[str], start: int, end: int) -> int:
    s = " ".join(words[start:end]).encode("utf-8", errors="ignore")
    digest = hashlib.sha1(s).digest()[:8]
    return int.from_bytes(digest, "big", signed=False)


@dataclass(frozen=True)
class ShingleSet:
    k: int
    words: list[str]
    ranges: list[tuple[int, int]]  # (start,end)
    hashes: list[int]              # aligned with ranges


def build_shingles(text: str, k: int) -> ShingleSet:
    w = tokenize_words(text)
    r = shingles(w, k)
    h = [shingle_hash(w, a, b) for (a, b) in r]
    return ShingleSet(k=k, words=w, ranges=r, hashes=h)
