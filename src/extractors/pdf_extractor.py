from pypdf import PdfReader


def extract_pdf(path: str) -> str:
    """
    Извлекает текст из текстового PDF (не скана) постранично и склеивает его.
    """
    try:
        reader = PdfReader(path)
        full_text = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                full_text.append(text)

        # Склеиваем страницы через перенос строки
        return '\n'.join(full_text)
    except Exception as e:
        print(f"Error reading PDF {path}: {e}")
        return ""