"""
Вспомогательные функции для UI
"""

import streamlit as st
import os

def validate_file(uploaded_file):
    """Проверка загруженного файла"""
    if uploaded_file is None:
        return False, "Файл не загружен"
    
    # Проверка типа файла
    allowed_extensions = ['.docx', '.pdf', '.txt']
    file_extension = os.path.splitext(uploaded_file.name)[1].lower()
    
    if file_extension not in allowed_extensions:
        return False, f"Неподдерживаемый формат файла: {file_extension}"
    
    # Проверка размера файла (макс 10 MB)
    max_size = 10 * 1024 * 1024  # 10 MB
    if uploaded_file.size > max_size:
        return False, f"Файл слишком большой (макс. {max_size/1024/1024} MB)"
    
    return True, "Файл валиден"

def validate_corpus_path(corpus_path):
    """Проверка пути к папке с документами"""
    if not corpus_path:
        return False, "Путь не указан"
    
    # В MVP просто проверяем, что путь не пустой
    # В реальной системе проверяли бы существование папки
    return True, "Путь указан"

def display_progress_bar(stage, total_stages=5):
    """Отображение прогресс-бара"""
    progress = stage / total_stages
    st.progress(progress)
    
    stages = [
        "Извлечение текста...",
        "Нормализация...", 
        "Быстрый поиск...",
        "Точное сравнение...",
        "Формирование отчета..."
    ]
    
    if stage <= len(stages):
        st.caption(f"📌 {stages[stage-1]}")
