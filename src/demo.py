import os
import difflib
from src.utils import extract_text
from src.normalize import normalize

# Пути к файлам (должны совпадать с теми, что генерирует generate_data.py)
TEST_DIR = "data/tests"
FILES = {
    "case_1": ("case_1_original.txt", "case_1_copy.txt", "100% копия"),
    "case_2": ("case_1_original.txt", "case_2_partial.txt", "Частичное вхождение (шум + текст)"),
    "case_3": ("case_1_original.txt", "case_3_typos.txt", "Опечатки и мелкие правки"),
    "case_4": ("case_1_original.txt", "case_4_reorder.txt", "Перестановка предложений"),
    "case_5": ("case_5_check.docx", "case_5_check.pdf", "Сравнение форматов (DOCX vs PDF)")
}


def calculate_similarity(text1: str, text2: str) -> dict:
    """
    Считает две метрики:
    1. SequenceMatcher (порядок важен) — для поиска точных копий и опечаток.
    2. Jaccard (пересечение слов) — для поиска текста при перестановке слов/предложений.
    """
    # 1. Схожесть последовательности (Levenshtein-like)
    seq_ratio = difflib.SequenceMatcher(None, text1, text2).ratio()

    # 2. Схожесть набора слов (Jaccard)
    set1 = set(text1.split())
    set2 = set(text2.split())
    if not set1 or not set2:
        jaccard = 0.0
    else:
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        jaccard = intersection / union

    return {
        "sequence": round(seq_ratio * 100, 1),
        "jaccard": round(jaccard * 100, 1)
    }


def run_full_demo():
    print("=== ЗАПУСК ПОЛНОЙ ПРОВЕРКИ ТЕСТ-КЕЙСОВ ===\n")

    for case_id, (file_ref, file_target, description) in FILES.items():
        path_ref = os.path.join(TEST_DIR, file_ref)
        path_target = os.path.join(TEST_DIR, file_target)

        # Проверка наличия файлов
        if not os.path.exists(path_ref) or not os.path.exists(path_target):
            print(f"[SKIP] {case_id}: Файлы не найдены. Запустите generate_data.py")
            continue

        print(f"--- {case_id.upper()}: {description} ---")

        # 1. Извлечение
        raw_ref = extract_text(path_ref)
        raw_target = extract_text(path_target)

        # 2. Нормализация
        norm_ref = normalize(raw_ref)
        norm_target = normalize(raw_target)

        # 3. Сравнение
        scores = calculate_similarity(norm_ref, norm_target)

        # Вывод результатов
        print(f"Файлы: {file_ref} <-> {file_target}")
        print(f"Схожесть (последовательность): {scores['sequence']}%")
        print(f"Схожесть (набор слов):         {scores['jaccard']}%")

        # Краткий анализ результатов для демо
        if scores['sequence'] == 100.0:
            print(">> РЕЗУЛЬТАТ: Идеальное совпадение.")
        elif scores['sequence'] > 85.0:
            print(">> РЕЗУЛЬТАТ: Высокое сходство (возможны опечатки).")
        elif scores['jaccard'] > 85.0 and scores['sequence'] < 80.0:
            print(">> РЕЗУЛЬТАТ: Текст переставлен местами (слова те же, порядок разный).")
        elif scores['sequence'] > 30.0:
            print(">> РЕЗУЛЬТАТ: Частичное совпадение (найден фрагмент).")
        else:
            print(">> РЕЗУЛЬТАТ: Тексты разные.")

        print("-" * 40 + "\n")


if __name__ == "__main__":
    run_full_demo()