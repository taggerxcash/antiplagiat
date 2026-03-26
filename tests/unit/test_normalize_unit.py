import unittest

from src.normalize import (
    fix_extraction_artifacts,
    normalize_symbols,
    normalize_text,
    sanitize_service_chars,
)


class NormalizeUnitTest(unittest.TestCase):
    def test_normalize_text_lowercase_spaces_and_paragraphs(self) -> None:
        raw = "  ПрИвЕт   Мир\t\t\n\n\nТЕСТ   "
        self.assertEqual(normalize_text(raw), "привет мир\n\nтест")

    def test_sanitize_service_chars_removes_hidden_chars(self) -> None:
        raw = "A\u00a0B\u200b\u2060C\u00adD\x00E\fF"
        self.assertEqual(sanitize_service_chars(raw), "A BCDE F")

    def test_fix_extraction_artifacts_joins_broken_words(self) -> None:
        raw = "нор-\nмализация  \n текста"
        self.assertEqual(fix_extraction_artifacts(raw), "нормализация\nтекста")

    def test_normalize_symbols_and_punctuation_cleanup(self) -> None:
        raw = "«Тест» — это… 'пример'!!!"
        self.assertEqual(normalize_text(raw), "тест это пример")

    def test_single_newlines_are_merged_but_paragraphs_remain(self) -> None:
        raw = "Строка 1\nСтрока 2\n\n\nСтрока 3"
        self.assertEqual(normalize_text(raw), "строка 1 строка 2\n\nстрока 3")

    def test_none_like_and_non_string_inputs(self) -> None:
        self.assertEqual(normalize_text(None), "")
        self.assertEqual(normalize_text(12345), "12345")

    def test_normalize_symbols_function(self) -> None:
        raw = "“text” — ‘quote’ …"
        self.assertEqual(normalize_symbols(raw), "\"text\" - 'quote' ...")


if __name__ == "__main__":
    unittest.main()
