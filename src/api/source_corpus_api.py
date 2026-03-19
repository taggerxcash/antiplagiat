from __future__ import annotations

from functools import lru_cache
import os
import sqlite3
from typing import Any
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Query, Response, status
from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.storage.models import SourceCorpus, SourceCorpusState
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
from src.storage.repository import SourceCorpusRepository


class SourceCorpusCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    external_id: str | None = Field(default=None, min_length=1)
    name: str = Field(min_length=1, max_length=255)
    root_path: str = Field(min_length=1)
    parameters: dict[str, Any] = Field(default_factory=dict)
    is_enabled: bool = True
    state: SourceCorpusState = SourceCorpusState.NEW
    total_docs: int = Field(default=0, ge=0)
    indexed_docs: int = Field(default=0, ge=0)
    failed_docs: int = Field(default=0, ge=0)
    last_error: str | None = None
    indexed_at: str | None = None

    @model_validator(mode="after")
    def validate_counters(self) -> "SourceCorpusCreateRequest":
        if self.indexed_docs > self.total_docs:
            raise ValueError("indexed_docs cannot be greater than total_docs")
        if self.failed_docs > self.total_docs:
            raise ValueError("failed_docs cannot be greater than total_docs")
        return self


class SourceCorpusUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=255)
    root_path: str | None = Field(default=None, min_length=1)
    parameters: dict[str, Any] | None = None
    is_enabled: bool | None = None
    state: SourceCorpusState | None = None
    total_docs: int | None = Field(default=None, ge=0)
    indexed_docs: int | None = Field(default=None, ge=0)
    failed_docs: int | None = Field(default=None, ge=0)
    last_error: str | None = None
    indexed_at: str | None = None

    @model_validator(mode="after")
    def validate_counters(self) -> "SourceCorpusUpdateRequest":
        if (
            self.total_docs is not None
            and self.indexed_docs is not None
            and self.indexed_docs > self.total_docs
        ):
            raise ValueError("indexed_docs cannot be greater than total_docs")
        if (
            self.total_docs is not None
            and self.failed_docs is not None
            and self.failed_docs > self.total_docs
        ):
            raise ValueError("failed_docs cannot be greater than total_docs")
        return self


class SourceCorpusResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    external_id: str
    name: str
    root_path: str
    parameters: dict[str, Any]
    is_enabled: bool
    state: SourceCorpusState
    total_docs: int
    indexed_docs: int
    failed_docs: int
    last_error: str | None
    created_at: str | None
    updated_at: str | None
    indexed_at: str | None


class SourceCorpusListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[SourceCorpusResponse]
    total: int
    limit: int
    offset: int


class SourceCorpusParametersCommandRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    parameters: dict[str, Any]
    merge: bool = True
    force: bool = False


class SourceCorpusStateCommandRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_state: SourceCorpusState
    reason: str | None = None
    force: bool = False


class SourceCorpusToggleCommandRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str | None = None


def _to_response(corpus: SourceCorpus) -> SourceCorpusResponse:
    if corpus.id is None:
        raise ValueError("Persisted corpus must have id.")

    return SourceCorpusResponse(
        id=corpus.id,
        external_id=corpus.external_id,
        name=corpus.name,
        root_path=corpus.root_path,
        parameters=corpus.parameters,
        is_enabled=corpus.is_enabled,
        state=corpus.state,
        total_docs=corpus.total_docs,
        indexed_docs=corpus.indexed_docs,
        failed_docs=corpus.failed_docs,
        last_error=corpus.last_error,
        created_at=corpus.created_at,
        updated_at=corpus.updated_at,
        indexed_at=corpus.indexed_at,
    )


@lru_cache(maxsize=1)
def _repository() -> SourceCorpusRepository:
    db_path = os.getenv("ANTIPLAGIAT_DB_PATH", "src/data/antiplagiat.db")
    return SourceCorpusRepository(db_path=db_path)


def get_repository() -> SourceCorpusRepository:
    return _repository()


def get_management_service(
    repo: SourceCorpusRepository = Depends(get_repository),
) -> SourceCorpusManagementService:
    return SourceCorpusManagementService(repository=repo)


def _raise_for_management_error(exc: Exception) -> None:
    if isinstance(exc, SourceCorpusNotFoundError):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    if isinstance(exc, (InvalidSourceCorpusStateTransitionError, SourceCorpusDisabledError)):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    raise exc


app = FastAPI(
    title="Antiplagiat Source Corpus API",
    version="1.0.0",
    description="REST API for source corpus management.",
)


@app.get("/health")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/api/v1/source-corpora",
    response_model=SourceCorpusResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_source_corpus(
    payload: SourceCorpusCreateRequest,
    repo: SourceCorpusRepository = Depends(get_repository),
) -> SourceCorpusResponse:
    corpus = SourceCorpus(
        external_id=payload.external_id or str(uuid4()),
        name=payload.name,
        root_path=payload.root_path,
        parameters=payload.parameters,
        is_enabled=payload.is_enabled,
        state=payload.state,
        total_docs=payload.total_docs,
        indexed_docs=payload.indexed_docs,
        failed_docs=payload.failed_docs,
        last_error=payload.last_error,
        indexed_at=payload.indexed_at,
    )
    try:
        created = repo.create_corpus(corpus)
    except sqlite3.IntegrityError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Source corpus with this external_id already exists.",
        ) from exc

    return _to_response(created)


@app.get("/api/v1/source-corpora", response_model=SourceCorpusListResponse)
def list_source_corpora(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    repo: SourceCorpusRepository = Depends(get_repository),
) -> SourceCorpusListResponse:
    items = repo.list_corpora(limit=limit, offset=offset)
    total = repo.count_corpora()
    return SourceCorpusListResponse(
        items=[_to_response(item) for item in items],
        total=total,
        limit=limit,
        offset=offset,
    )


@app.get("/api/v1/source-corpora/{external_id}", response_model=SourceCorpusResponse)
def get_source_corpus(
    external_id: str,
    repo: SourceCorpusRepository = Depends(get_repository),
) -> SourceCorpusResponse:
    corpus = repo.get_corpus_by_external_id(external_id)
    if corpus is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Source corpus not found.",
        )
    return _to_response(corpus)


@app.put("/api/v1/source-corpora/{external_id}", response_model=SourceCorpusResponse)
def update_source_corpus(
    external_id: str,
    payload: SourceCorpusUpdateRequest,
    repo: SourceCorpusRepository = Depends(get_repository),
) -> SourceCorpusResponse:
    update_fields = payload.model_dump(exclude_unset=True)
    if not update_fields:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one field must be provided for update.",
        )

    updated = repo.update_corpus_fields(external_id=external_id, fields=update_fields)
    if updated is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Source corpus not found.",
        )
    return _to_response(updated)


@app.delete("/api/v1/source-corpora/{external_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_source_corpus(
    external_id: str,
    repo: SourceCorpusRepository = Depends(get_repository),
) -> Response:
    deleted = repo.delete_corpus(external_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Source corpus not found.",
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post(
    "/api/v1/source-corpora/{external_id}/commands/enable",
    response_model=SourceCorpusResponse,
)
def enable_source_corpus(
    external_id: str,
    payload: SourceCorpusToggleCommandRequest | None = None,
    service: SourceCorpusManagementService = Depends(get_management_service),
) -> SourceCorpusResponse:
    try:
        updated = service.enable(
            EnableSourceCorpusCommand(
                external_id=external_id,
                reason=payload.reason if payload else None,
            )
        )
    except Exception as exc:
        _raise_for_management_error(exc)
        raise
    return _to_response(updated)


@app.post(
    "/api/v1/source-corpora/{external_id}/commands/disable",
    response_model=SourceCorpusResponse,
)
def disable_source_corpus(
    external_id: str,
    payload: SourceCorpusToggleCommandRequest | None = None,
    service: SourceCorpusManagementService = Depends(get_management_service),
) -> SourceCorpusResponse:
    try:
        updated = service.disable(
            DisableSourceCorpusCommand(
                external_id=external_id,
                reason=payload.reason if payload else None,
            )
        )
    except Exception as exc:
        _raise_for_management_error(exc)
        raise
    return _to_response(updated)


@app.post(
    "/api/v1/source-corpora/{external_id}/commands/parameters",
    response_model=SourceCorpusResponse,
)
def update_source_corpus_parameters(
    external_id: str,
    payload: SourceCorpusParametersCommandRequest,
    service: SourceCorpusManagementService = Depends(get_management_service),
) -> SourceCorpusResponse:
    try:
        updated = service.update_parameters(
            UpdateSourceCorpusParametersCommand(
                external_id=external_id,
                parameters=payload.parameters,
                merge=payload.merge,
                force=payload.force,
            )
        )
    except Exception as exc:
        _raise_for_management_error(exc)
        raise
    return _to_response(updated)


@app.post(
    "/api/v1/source-corpora/{external_id}/commands/state",
    response_model=SourceCorpusResponse,
)
def change_source_corpus_state(
    external_id: str,
    payload: SourceCorpusStateCommandRequest,
    service: SourceCorpusManagementService = Depends(get_management_service),
) -> SourceCorpusResponse:
    try:
        updated = service.change_state(
            ChangeSourceCorpusStateCommand(
                external_id=external_id,
                target_state=payload.target_state,
                reason=payload.reason,
                force=payload.force,
            )
        )
    except Exception as exc:
        _raise_for_management_error(exc)
        raise
    return _to_response(updated)
