PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS source_corpora (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    root_path TEXT NOT NULL,
    parameters_json TEXT NOT NULL DEFAULT '{}',
    is_enabled INTEGER NOT NULL DEFAULT 1 CHECK (is_enabled IN (0, 1)),
    status TEXT NOT NULL CHECK (status IN ('new', 'indexing', 'ready', 'failed', 'archived')),
    total_docs INTEGER NOT NULL DEFAULT 0 CHECK (total_docs >= 0),
    indexed_docs INTEGER NOT NULL DEFAULT 0 CHECK (indexed_docs >= 0),
    failed_docs INTEGER NOT NULL DEFAULT 0 CHECK (failed_docs >= 0),
    last_error TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    indexed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_source_corpora_root_path
    ON source_corpora (root_path);

CREATE TABLE IF NOT EXISTS source_corpus_documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    corpus_id INTEGER NOT NULL,
    external_id TEXT NOT NULL UNIQUE,
    relative_path TEXT NOT NULL,
    absolute_path TEXT NOT NULL,
    text_hash TEXT NOT NULL,
    text_length INTEGER NOT NULL CHECK (text_length >= 0),
    normalized_text TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'excluded', 'failed')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (corpus_id) REFERENCES source_corpora(id) ON DELETE CASCADE,
    UNIQUE (corpus_id, relative_path)
);

CREATE INDEX IF NOT EXISTS idx_source_corpus_documents_corpus_id
    ON source_corpus_documents (corpus_id);

CREATE INDEX IF NOT EXISTS idx_source_corpus_documents_state
    ON source_corpus_documents (state);

CREATE TABLE IF NOT EXISTS source_corpus_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    corpus_id INTEGER NOT NULL,
    entity_type TEXT NOT NULL,
    entity_external_id TEXT NOT NULL,
    relation_type TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (corpus_id) REFERENCES source_corpora(id) ON DELETE CASCADE,
    UNIQUE (corpus_id, entity_type, entity_external_id, relation_type)
);

CREATE INDEX IF NOT EXISTS idx_source_corpus_links_corpus_id
    ON source_corpus_links (corpus_id);
