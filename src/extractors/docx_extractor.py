import docx

def extract_docx(path: str) -> str:
    """
    Извлекает текст из .docx файла, проходя по всем параграфам.
    """
    try:
        doc = docx.Document(path)
        full_text = []
        for para in doc.paragraphs:
            # Игнорируем совсем пустые строки, если нужно
            if para.text.strip():
                full_text.append(para.text)
        return '\n'.join(full_text)
    except Exception as e:
        print(f"Error reading DOCX {path}: {e}")
        return ""