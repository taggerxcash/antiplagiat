from dataclasses import dataclass

@dataclass(frozen=True)
class CoreConfig:
    # Fast layer (TF-IDF char n-grams)
    char_ngram_min: int = 3
    char_ngram_max: int = 5
    max_features: int | None = 200_000  # None = unlimited
    min_df: int = 1
    top_k: int = 10

    # Exact layer (word shingles)
    shingle_k: int = 5
    merge_gap_words: int = 2          # разрешенный разрыв при склейке блоков
    max_blocks_per_doc: int = 5       # сколько блоков показываем в отчёте
    min_block_words: int = 20         # отсекаем слишком короткие блоки
    weight_fast: float = 0.4
    weight_exact: float = 0.6


    # Corpus loading
    allowed_exts: tuple[str, ...] = (".txt", ".docx", ".pdf")
