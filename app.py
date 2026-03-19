import streamlit as st
import pandas as pd
import time
import tempfile
import json
from pathlib import Path
from uuid import uuid4

from src.utils import extract_text
from src.core.pipeline import run_full_stage
from src.storage.management import (
    ChangeSourceCorpusStateCommand,
    DisableSourceCorpusCommand,
    EnableSourceCorpusCommand,
    InvalidSourceCorpusStateTransitionError,
    SourceCorpusDisabledError,
    SourceCorpusManagementService,
    SourceCorpusNotFoundError,
    UpdateSourceCorpusParametersCommand,
)
from src.storage.models import SourceCorpus, SourceCorpusState
from src.storage.repository import SourceCorpusRepository


try:
    from utils import validate_file, validate_corpus_path, display_progress_bar
except ImportError:
    def validate_file(file): return True, "OK"
    def validate_corpus_path(path): return True, "OK"
    def display_progress_bar(stage): pass

def render_progress(stage: int, placeholder):
    labels = {
        1: "1/5 Подготовка входных данных",
        2: "2/5 Загрузка корпуса",
        3: "3/5 Быстрый поиск кандидатов (TF-IDF)",
        4: "4/5 Точная проверка (шинглы)",
        5: "5/5 Формирование отчёта",
    }
    progress = stage / 5.0
    with placeholder.container():
        st.subheader("📊 Ход проверки")
        st.progress(progress)
        st.caption(labels.get(stage, f"{stage}/5"))


@st.cache_resource
def get_source_corpus_repo() -> SourceCorpusRepository | None:
    try:
        return SourceCorpusRepository()
    except Exception:
        return None


@st.cache_resource
def get_source_corpus_management_service() -> SourceCorpusManagementService | None:
    repo = get_source_corpus_repo()
    if repo is None:
        return None
    return SourceCorpusManagementService(repository=repo)


def _parse_json_dict(raw: str, field_name: str) -> dict:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field_name}: некорректный JSON ({exc})") from exc

    if not isinstance(data, dict):
        raise ValueError(f"{field_name}: ожидается JSON-объект.")
    return data


def _format_corpus_label(corpus: SourceCorpus) -> str:
    status_icon = "🟢" if corpus.is_enabled else "⚪"
    return f"{status_icon} {corpus.name} [{corpus.state.value}] ({corpus.external_id[:8]})"


def _show_management_error(exc: Exception) -> None:
    if isinstance(exc, SourceCorpusNotFoundError):
        st.error("Корпус не найден.")
    elif isinstance(exc, (InvalidSourceCorpusStateTransitionError, SourceCorpusDisabledError)):
        st.error(str(exc))
    else:
        st.error(f"Ошибка выполнения команды: {exc}")


def render_source_corpus_management_ui() -> None:
    st.markdown("---")
    st.header("🗂️ Управление корпусом источника (демо)")

    repo = get_source_corpus_repo()
    service = get_source_corpus_management_service()

    if repo is None or service is None:
        st.warning("Хранилище корпусов недоступно. Проверьте доступ к SQLite-файлу.")
        return

    corpora = repo.list_corpora(limit=200, offset=0)

    if corpora:
        rows = []
        for c in corpora:
            rows.append(
                {
                    "Корпус": c.name,
                    "External ID": c.external_id,
                    "Состояние": c.state.value,
                    "Включен": "Да" if c.is_enabled else "Нет",
                    "Документов": c.total_docs,
                    "Индексировано": c.indexed_docs,
                    "Ошибок": c.failed_docs,
                    "Обновлён": c.updated_at,
                }
            )
        st.dataframe(pd.DataFrame(rows), use_container_width=True)
    else:
        st.info("Пока нет корпусов. Создайте первый корпус через форму ниже.")

    col_create, col_manage = st.columns([1, 1.2])

    with col_create:
        st.subheader("Создание корпуса")
        with st.form("create_source_corpus_form"):
            create_name = st.text_input("Название", value="Новый корпус")
            create_root = st.text_input("Путь к папке корпуса", value="src/data/corpus")
            create_external_id = st.text_input("External ID (опционально)", value="").strip()
            create_state = st.selectbox(
                "Начальное состояние",
                options=[s.value for s in SourceCorpusState],
                index=0,
            )
            create_enabled = st.checkbox("Корпус включен", value=True)
            create_params_raw = st.text_area(
                "Параметры (JSON)",
                value='{"language": "ru", "min_similarity": 0.7}',
                height=140,
            )
            create_submitted = st.form_submit_button("Создать корпус", type="primary")

            if create_submitted:
                try:
                    params = _parse_json_dict(create_params_raw, "Параметры")
                    created = repo.create_corpus(
                        SourceCorpus(
                            name=create_name.strip() or "Новый корпус",
                            root_path=create_root.strip() or "src/data/corpus",
                            parameters=params,
                            is_enabled=create_enabled,
                            state=SourceCorpusState(create_state),
                            external_id=create_external_id or str(uuid4()),
                        )
                    )
                except Exception as exc:
                    st.error(f"Не удалось создать корпус: {exc}")
                else:
                    st.success(f"Корпус создан: {created.name} ({created.external_id})")
                    st.rerun()

    with col_manage:
        st.subheader("Управление существующим корпусом")
        if not corpora:
            st.caption("После создания корпуса здесь появятся элементы управления.")
            return

        corpus_by_id = {c.external_id: c for c in corpora}
        selected_external_id = st.selectbox(
            "Выберите корпус",
            options=list(corpus_by_id.keys()),
            format_func=lambda eid: _format_corpus_label(corpus_by_id[eid]),
        )
        selected = corpus_by_id[selected_external_id]

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Состояние", selected.state.value)
        m2.metric("Включен", "Да" if selected.is_enabled else "Нет")
        m3.metric("Индексировано", f"{selected.indexed_docs}/{selected.total_docs}")
        m4.metric("Ошибок", str(selected.failed_docs))

        st.caption(f"Путь: {selected.root_path}")
        st.caption(f"Обновлён: {selected.updated_at or '-'} | Индексирован: {selected.indexed_at or '-'}")
        if selected.last_error:
            st.warning(f"Последняя ошибка: {selected.last_error}")

        st.markdown("**Текущие параметры корпуса**")
        st.code(json.dumps(selected.parameters, ensure_ascii=False, indent=2), language="json")

        with st.form("toggle_corpus_form"):
            toggle_reason = st.text_input("Причина (опционально)", value="")
            t1, t2 = st.columns(2)
            enable_submitted = t1.form_submit_button("Включить")
            disable_submitted = t2.form_submit_button("Выключить")

            if enable_submitted:
                try:
                    service.enable(
                        EnableSourceCorpusCommand(
                            external_id=selected_external_id,
                            reason=toggle_reason.strip() or None,
                        )
                    )
                except Exception as exc:
                    _show_management_error(exc)
                else:
                    st.success("Корпус включен.")
                    st.rerun()

            if disable_submitted:
                try:
                    service.disable(
                        DisableSourceCorpusCommand(
                            external_id=selected_external_id,
                            reason=toggle_reason.strip() or None,
                        )
                    )
                except Exception as exc:
                    _show_management_error(exc)
                else:
                    st.success("Корпус выключен.")
                    st.rerun()

        with st.form("change_state_form"):
            target_state = st.selectbox(
                "Новое состояние",
                options=[s.value for s in SourceCorpusState],
                index=[s.value for s in SourceCorpusState].index(selected.state.value),
            )
            state_reason = st.text_input("Причина смены состояния", value="")
            state_force = st.checkbox("Принудительно (force)", value=False)
            state_submitted = st.form_submit_button("Изменить состояние")

            if state_submitted:
                try:
                    service.change_state(
                        ChangeSourceCorpusStateCommand(
                            external_id=selected_external_id,
                            target_state=SourceCorpusState(target_state),
                            reason=state_reason.strip() or None,
                            force=state_force,
                        )
                    )
                except Exception as exc:
                    _show_management_error(exc)
                else:
                    st.success("Состояние корпуса обновлено.")
                    st.rerun()

        with st.form("update_params_form"):
            params_raw = st.text_area(
                "Новые параметры (JSON)",
                value=json.dumps(selected.parameters, ensure_ascii=False, indent=2),
                height=180,
            )
            p1, p2 = st.columns(2)
            merge_params = p1.checkbox("Сливать с текущими (merge)", value=True)
            force_params = p2.checkbox("Принудительно (force)", value=False)
            params_submitted = st.form_submit_button("Применить параметры")

            if params_submitted:
                try:
                    payload = _parse_json_dict(params_raw, "Параметры")
                    service.update_parameters(
                        UpdateSourceCorpusParametersCommand(
                            external_id=selected_external_id,
                            parameters=payload,
                            merge=merge_params,
                            force=force_params,
                        )
                    )
                except Exception as exc:
                    _show_management_error(exc)
                else:
                    st.success("Параметры корпуса обновлены.")
                    st.rerun()


def main():
    st.set_page_config(
        page_title="Проверка документов",
        layout="wide"
    )

    if "last_report" not in st.session_state:
        st.session_state["last_report"] = None

    
    st.title("🔍 Антиплагиат")
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.header("📄 Проверяемый документ")
        uploaded_file = st.file_uploader(
            "Загрузите документ для проверки",
            type=["docx", "pdf", "txt"],
            help="Поддерживаются файлы формата .docx .pdf .txt"
        )
        
        if uploaded_file:
            st.success(f"✅ Загружен: {uploaded_file.name}")
            file_details = {
                "Имя файла": uploaded_file.name,
                "Тип файла": uploaded_file.type,
                "Размер": f"{uploaded_file.size / 1024:.1f} KB"
            }
            st.json(file_details)
    
    with col2:
        st.header("📁 Папка с источниками")
        corpus_path = st.text_input(
            "Путь к папке с документами:",
            placeholder="C:/Documents/Corpus  или  /home/user/documents",
            help="Укажите полный путь к папке с исходными документами"
        )
        
        use_example = st.checkbox("Использовать тестовые данные", value=True)
        if use_example:
            st.info("Будут использованы тестовые данные из папки test_data/")
    
    st.markdown("---")
    
    st.header("Запуск проверки")

    check_button = st.button(
        "НАЧАТЬ ПРОВЕРКУ",
        type="primary",
        use_container_width=True
    )

    
    if check_button:
        errors = []
        
        if not uploaded_file and not use_example:
            errors.append("Загрузите документ для проверки")
        elif uploaded_file:
            is_valid, message = validate_file(uploaded_file)
            if not is_valid:
                errors.append(message)
        
        if not corpus_path and not use_example:
            errors.append("Укажите путь к папке с документами")
        elif corpus_path:
            is_valid, message = validate_corpus_path(corpus_path)
            if not is_valid:
                errors.append(message)
        
        if errors:
            for error in errors:
                st.error(f"❌ {error}")
        else:
            progress_placeholder = st.empty()

            render_progress(1, progress_placeholder)

            if use_example and not uploaded_file:
                query_path = "src/data/tests/case_1_copy.txt"
                query_raw_text = extract_text(query_path)
            else:
                temp_path = save_uploaded_to_temp(uploaded_file)
                query_raw_text = extract_text(temp_path)

            time.sleep(0.35)

            render_progress(2, progress_placeholder)
            if use_example and not corpus_path:
                corpus_dir = "src/data/corpus"
            else:
                corpus_dir = corpus_path

            time.sleep(0.25)

            render_progress(3, progress_placeholder)
            report = run_full_stage(
                query_raw_text,
                corpus_dir,
                corpus_repo=get_source_corpus_repo(),
            )
            st.session_state["last_report"] = report

            time.sleep(0.15)

            render_progress(4, progress_placeholder)
            time.sleep(0.05)

            render_progress(5, progress_placeholder)
            st.success("✅ Проверка завершена!")

            progress_placeholder.empty()

            show_full_results(report)

    if (not check_button) and (st.session_state["last_report"] is not None):
        show_full_results(st.session_state["last_report"])

    
    st.markdown("---")
    st.info("""
    **ℹ️ Информация о системе:**
    - Поддерживаемые форматы: DOCX, PDF, TXT
    - Проверяется текстовое сходство документов
    - Результаты включают процент сходства и найденные фрагменты
    """)

    render_source_corpus_management_ui()

def show_placeholder_results():
    """Заглушка для отображения результатов"""
    st.header("📊 Результаты проверки")
    
    tab1, tab2 = st.tabs(["📈 Сводка", "🔍 Фрагменты"])
    
    with tab1:
        st.subheader("Топ совпадений")
        
        data = {
            "Файл источника": ["report_2023.docx", "thesis.pdf", "manual_v2.docx", "research_paper.pdf"],
            "Сходство TF-IDF": [0.92, 0.87, 0.75, 0.68],
            "Сходство шинглов (%)": [89, 82, 71, 65],
            "Совпавших фрагментов": [15, 12, 8, 5]
        }
        
        df = pd.DataFrame(data)
        
        styled_df = df.style.format({
            "Сходство TF-IDF": "{:.2f}",
            "Сходство шинглов (%)": "{:.0f}%"
        })
        
        st.dataframe(styled_df, use_container_width=True)
        
        st.subheader("Распределение сходства")
        chart_data = pd.DataFrame({
            "Диапазон сходства": ["0-20%", "20-40%", "40-60%", "60-80%", "80-100%"],
            "Количество документов": [3, 5, 8, 4, 2]
        })
        st.bar_chart(chart_data.set_index("Диапазон сходства"))
    
    with tab2:
        st.subheader("Найденные совпадения")
        
        matches = [
            {
                "source": "report_2023.docx",
                "similarity": 89,
                "text": "В данном исследовании рассматриваются методы анализа текстовых данных с использованием машинного обучения. Особое внимание уделяется алгоритмам классификации и кластеризации.",
                "context": "стр. 5-6, раздел 'Методология'"
            },
            {
                "source": "thesis.pdf", 
                "similarity": 82,
                "text": "Результаты экспериментов показывают, что предложенный метод превосходит существующие аналоги на 15-20% по точности классификации при сохранении приемлемой скорости работы.",
                "context": "стр. 23, глава 'Результаты'"
            },
            {
                "source": "manual_v2.docx",
                "similarity": 71,
                "text": "Для нормализации текста используются следующие шаги: приведение к нижнему регистру, удаление стоп-слов, лемматизация и обработка специальных символов.",
                "context": "стр. 8, раздел 'Предобработка'"
            }
        ]
        
        for i, match in enumerate(matches, 1):
            with st.expander(f"Совпадение #{i}: {match['source']} ({match['similarity']}%)"):
                st.markdown(f"**Источник:** {match['source']}")
                st.markdown(f"**Контекст:** {match['context']}")
                st.markdown(f"**Сходство:** {match['similarity']}%")
                st.markdown("**Текст совпадения:**")
                st.info(match['text'])
                
                col1, col2 = st.columns(2)
                with col1:
                    st.button(f"Открыть источник", key=f"open_{i}")
                with col2:
                    st.button(f"Скрыть фрагмент", key=f"hide_{i}")

def save_uploaded_to_temp(uploaded_file) -> str:
    """
    Streamlit UploadedFile -> временный файл на диске -> возвращаем путь.
    """
    suffix = Path(uploaded_file.name).suffix.lower()
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(uploaded_file.getbuffer())
        return tmp.name

def show_fast_results(result: dict):
    st.header("📊 Результаты проверки (fast layer)")

    tab1, tab2 = st.tabs(["📈 Сводка", "🔍 Фрагменты"])

    with tab1:
        st.subheader("Топ совпадений (TF-IDF char n-grams)")

        df = pd.DataFrame(result["candidates"])
        if df.empty:
            st.warning("В корпусе не найдено кандидатов (пустой корпус или пустой текст).")
            return

        df = df.rename(columns={"path": "Файл источника", "score_fast": "Сходство TF-IDF"})
        df["Сходство TF-IDF"] = df["Сходство TF-IDF"].astype(float).round(3)

        st.write(f"Корпус: {result['corpus_size']} документов | Длина запроса: {result['query_len']} символов")
        st.dataframe(df, use_container_width=True)

        st.subheader("Распределение сходства (top-K)")
        chart_df = df.copy()
        chart_df["Файл источника"] = chart_df["Файл источника"].apply(lambda p: Path(p).name)
        st.bar_chart(chart_df.set_index("Файл источника")["Сходство TF-IDF"])

    with tab2:
        st.info("Точный слой (шинглы + восстановление совпадающих блоков) ещё не подключён. "
                "Здесь появятся совпадающие фрагменты после следующего шага.")

def show_full_results(report: dict):
    st.header("📊 Результаты проверки")
    if report["results"]:
        top = report["results"][0]
        st.subheader(f"Итог: {top['score_final']*100:.1f}% сходства с топ-источником")
        st.caption(f"Топ-источник: {Path(top['path']).name} | TF-IDF={top['score_fast']:.3f} | Шинглы={top['score_exact']*100:.1f}% | Блоков={len(top['blocks'])}")


    tab1, tab2 = st.tabs(["📈 Сводка", "🔍 Фрагменты"])

    with tab1:
        rows = []
        for r in report["results"]:
            rows.append({
                "Файл источника": r["path"].replace("\\", "/").split("/")[-1],
                "Итоговое сходство (%)": r["score_final"] * 100,
                "TF-IDF": r["score_fast"],
                "Шинглы (%)": r["score_exact"] * 100,
                "Блоков": len(r["blocks"]),
            })

        df = pd.DataFrame(rows)
        if df.empty:
            st.warning("Нет результатов.")
            return

        df["Итоговое сходство (%)"] = df["Итоговое сходство (%)"].round(1).astype(str) + "%"
        df["TF-IDF"] = df["TF-IDF"].round(3)
        df["Шинглы (%)"] = df["Шинглы (%)"].round(1).astype(str) + "%"

        st.dataframe(df, use_container_width=True)

    with tab2:
        if not report["results"]:
            st.warning("Нет результатов.")
            return

        min_final = st.slider("Минимальный итоговый скор для показа", 0.0, 1.0, 0.7, 0.05)

        for i, r in enumerate(report["results"], 1):
            if r["score_final"] < min_final:
                continue
            fname = r["path"].replace("\\", "/").split("/")[-1]
            title = f"#{i} {fname} | итог={r['score_final']:.3f} | tfidf={r['score_fast']:.3f} | sh={r['score_exact']*100:.1f}%"
            with st.expander(title):
                pairs = r.get("pairs", [])

                if pairs:
                    st.subheader("Пары совпадений: проверяемый документ vs источник")

                    for j, p in enumerate(pairs, 1):
                        st.markdown(f"### Пара {j}")

                        c1, c2 = st.columns(2)
                        with c1:
                            st.markdown("**Проверяемый документ (контекст):**")
                            st.info(p["query"].get("context", ""))
                            st.markdown("**Совпадающий фрагмент:**")
                            st.success(p["query"].get("text", ""))

                        with c2:
                            st.markdown("**Источник (контекст):**")
                            st.info(p["source"].get("context", ""))
                            st.markdown("**Совпадающий фрагмент:**")
                            st.success(p["source"].get("text", ""))

                else:
                    if not r.get("blocks"):
                        st.info("Совпадающих блоков (достаточной длины) не найдено.")
                    else:
                        st.subheader("Совпадения в источнике (fallback)")
                        for j, b in enumerate(r["blocks"], 1):
                            st.markdown(f"**Блок {j}:** слова {b['start_word']}–{b['end_word']} (≈{b['words']} слов)")
                            st.markdown("**Контекст (±20 слов):**")
                            st.info(b.get("context", ""))
                            st.markdown("**Совпадающий фрагмент:**")
                            st.success(b.get("text", ""))

if __name__ == "__main__":
    main()
