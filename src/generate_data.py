import os
import shutil  # Добавлено для удаления папок
import random
import docx
from fpdf import FPDF

# Конфигурация путей
BASE_DIR = "data"
CORPUS_DIR = os.path.join(BASE_DIR, "corpus")
TESTS_DIR = os.path.join(BASE_DIR, "tests")

# --- 0. Очистка и создание директорий ---
print("Очистка старых данных...")
if os.path.exists(CORPUS_DIR):
    shutil.rmtree(CORPUS_DIR)
if os.path.exists(TESTS_DIR):
    shutil.rmtree(TESTS_DIR)

os.makedirs(CORPUS_DIR, exist_ok=True)
os.makedirs(TESTS_DIR, exist_ok=True)
print("Директории готовы.")

# Базовый текст для генерации
BASE_TEXT = """
Системный анализ — это научный метод познания, представляющий собой последовательность действий 
по установлению структурных связей между переменными или элементами исследуемой системы. 
Опирается на комплекс общенаучных, экспериментальных, естественнонаучных, статистических, математических методов.
"""

# --- Утилиты создания файлов ---

def create_docx(path, text):
    doc = docx.Document()
    doc.add_paragraph(text)
    doc.save(path)

def create_pdf(path, text):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    # encode/decode hack для совместимости с FPDF без внешних шрифтов (latin-1)
    # В реальном проекте нужно подключать TTF шрифт с кириллицей.
    safe_text = text.encode('latin-1', 'replace').decode('latin-1')
    pdf.multi_cell(0, 10, txt=safe_text)
    pdf.output(path)

def create_txt(path, text):
    with open(path, 'w', encoding='utf-8') as f:
        f.write(text)

# --- 1. Генерация Корпуса (20 файлов) ---
print("Генерация корпуса...")
for i in range(1, 21):
    content = f"Документ номер {i}. {BASE_TEXT} Случайное число: {random.randint(0, 1000)}."
    if i % 3 == 0:
        create_docx(f"{CORPUS_DIR}/doc_{i}.docx", content)
    elif i % 3 == 1:
        create_txt(f"{CORPUS_DIR}/doc_{i}.txt", content)
    else:
        create_txt(f"{CORPUS_DIR}/doc_{i}_fake_pdf.txt", content)

# --- 2. Генерация Тест-кейсов ---
print("Генерация тестов...")

# Кейс 1: 100% копия
create_txt(f"{TESTS_DIR}/case_1_original.txt", BASE_TEXT)
create_txt(f"{TESTS_DIR}/case_1_copy.txt", BASE_TEXT)

# Кейс 2: Копипаст абзаца (вставка в середину)
noise = "Какой-то шумный текст в начале. "
create_txt(f"{TESTS_DIR}/case_2_partial.txt", noise + BASE_TEXT + " Еще текст.")

# Кейс 3: Мелкие правки (опечатки)
modified_text = BASE_TEXT.replace("научный", "нау4ный").replace("системы", "сисиемы")
create_txt(f"{TESTS_DIR}/case_3_typos.txt", modified_text)

# Кейс 4: Перестановка предложений
sentences = BASE_TEXT.replace('\n', ' ').split('.')
sentences = [s.strip() for s in sentences if s.strip()]
random.shuffle(sentences)
reordered = '. '.join(sentences) + '.'
create_txt(f"{TESTS_DIR}/case_4_reorder.txt", reordered)

# Кейс 5: Проверка парсинга (PDF vs DOCX с одинаковым текстом)
check_text = "This is a standard text for format checking."
create_docx(f"{TESTS_DIR}/case_5_check.docx", check_text)
create_pdf(f"{TESTS_DIR}/case_5_check.pdf", check_text)

print(f"Данные успешно пересозданы в {BASE_DIR}/")