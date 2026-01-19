import os
from src.extractors.docx_extractor import extract_docx
from src.extractors.pdf_extractor import extract_pdf

def extract_text(path: str) -> str:
    """
    Определяет тип файла по расширению и вызывает нужный экстрактор.
    """
    _, ext = os.path.splitext(path)
    ext = ext.lower()

    if ext == '.docx':
        return extract_docx(path)
    elif ext == '.pdf':
        return extract_pdf(path)
    elif ext == '.txt':
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            print(f"Error reading TXT {path}: {e}")
            return ""
    else:
        print(f"Unsupported file format: {ext}")
        return ""