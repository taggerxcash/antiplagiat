from __future__ import annotations

import re
import unicodedata

_REMOVABLE_SERVICE_CHARS = {
    "\u200b",  # zero width space
    "\u200c",  # zero width non-joiner
    "\u200d",  # zero width joiner
    "\ufeff",  # BOM / zero width no-break space
    "\u2060",  # word joiner
    "\u00ad",  # soft hyphen
}

_SPACE_NORMALIZATION_MAP = str.maketrans(
    {
        "\u00a0": " ",  # no-break space
        "\u2007": " ",  # figure space
        "\u202f": " ",  # narrow no-break space
        "\t": " ",
        "\f": " ",
        "\v": " ",
    }
)

_SYMBOL_NORMALIZATION_MAP = str.maketrans(
    {
        "«": '"',
        "»": '"',
        "“": '"',
        "”": '"',
        "„": '"',
        "‟": '"',
        "″": '"',
        "’": "'",
        "‘": "'",
        "‚": "'",
        "‛": "'",
        "`": "'",
        "´": "'",
        "—": "-",
        "–": "-",
        "‒": "-",
        "―": "-",
        "−": "-",
        "…": "...",
    }
)

_BROKEN_HYPHEN_WORD_RE = re.compile(r"(?<=\w)-\s*\n\s*(?=\w)", re.UNICODE)
_MULTI_SPACES_RE = re.compile(r"[ ]{2,}")
_MULTI_NEWLINES_RE = re.compile(r"\n{3,}")
_SINGLE_NEWLINE_RE = re.compile(r"(?<!\n)\n(?!\n)")
_PUNCT_RE = re.compile(r"[^\w\s\n]", re.UNICODE)


def sanitize_service_chars(text: str | None) -> str:
    if text is None:
        return ""

    if not isinstance(text, str):
        text = str(text)

    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.translate(_SPACE_NORMALIZATION_MAP)

    cleaned_chars: list[str] = []
    for ch in text:
        if ch in _REMOVABLE_SERVICE_CHARS:
            continue
        if ch == "\n":
            cleaned_chars.append(ch)
            continue

        # Category "C*" covers control/non-display characters.
        if unicodedata.category(ch).startswith("C"):
            continue
        else:
            cleaned_chars.append(ch)

    return "".join(cleaned_chars)


def fix_extraction_artifacts(text: str) -> str:
    text = _BROKEN_HYPHEN_WORD_RE.sub("", text)
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = _MULTI_SPACES_RE.sub(" ", text)
    text = _MULTI_NEWLINES_RE.sub("\n\n", text)
    return text


def normalize_symbols(text: str) -> str:
    return text.translate(_SYMBOL_NORMALIZATION_MAP)


def normalize_text(text: str | None) -> str:
    """
    Многошаговая базовая нормализация:
    1. Очистка служебных/неотображаемых символов.
    2. Исправление артефактов извлечения (PDF/DOCX).
    3. Унификация спецсимволов.
    4. Приведение к lower + почти полное удаление пунктуации.
    5. Нормализация пробелов/абзацев с сохранением абзацной структуры.
    """
    prepared = sanitize_service_chars(text)
    if not prepared:
        return ""

    prepared = fix_extraction_artifacts(prepared)
    prepared = normalize_symbols(prepared)
    prepared = prepared.lower()

    # Удаляем почти всю пунктуацию, оставляя буквы/цифры/пробелы/переносы.
    prepared = _PUNCT_RE.sub(" ", prepared)

    prepared = re.sub(r"[ \t\f\v]+", " ", prepared)
    prepared = re.sub(r" *\n *", "\n", prepared)
    prepared = _MULTI_NEWLINES_RE.sub("\n\n", prepared)

    # Одиночные переносы (внутри абзацев) превращаем в пробел.
    prepared = _SINGLE_NEWLINE_RE.sub(" ", prepared)
    prepared = re.sub(r" *\n *\n *", "\n\n", prepared)
    prepared = _MULTI_SPACES_RE.sub(" ", prepared)

    paragraphs = [p.strip() for p in prepared.split("\n\n")]
    paragraphs = [p for p in paragraphs if p]
    return "\n\n".join(paragraphs).strip()


# Backward compatibility for old imports (src/demo.py).
normalize = normalize_text
