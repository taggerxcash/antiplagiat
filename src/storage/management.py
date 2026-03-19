from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Any

from src.storage.models import SourceCorpus, SourceCorpusState
from src.storage.repository import SourceCorpusRepository


class SourceCorpusManagementError(RuntimeError):
    pass


class SourceCorpusNotFoundError(SourceCorpusManagementError):
    pass


class InvalidSourceCorpusStateTransitionError(SourceCorpusManagementError):
    pass


class SourceCorpusDisabledError(SourceCorpusManagementError):
    pass


@dataclass(frozen=True)
class EnableSourceCorpusCommand:
    external_id: str
    reason: str | None = None


@dataclass(frozen=True)
class DisableSourceCorpusCommand:
    external_id: str
    reason: str | None = None


@dataclass(frozen=True)
class UpdateSourceCorpusParametersCommand:
    external_id: str
    parameters: dict[str, Any]
    merge: bool = True
    force: bool = False


@dataclass(frozen=True)
class ChangeSourceCorpusStateCommand:
    external_id: str
    target_state: SourceCorpusState
    reason: str | None = None
    force: bool = False


ManagementCommand = (
    EnableSourceCorpusCommand
    | DisableSourceCorpusCommand
    | UpdateSourceCorpusParametersCommand
    | ChangeSourceCorpusStateCommand
)


class SourceCorpusManagementService:
    _ALLOWED_TRANSITIONS: dict[SourceCorpusState, set[SourceCorpusState]] = {
        SourceCorpusState.NEW: {
            SourceCorpusState.INDEXING,
            SourceCorpusState.READY,
            SourceCorpusState.FAILED,
            SourceCorpusState.ARCHIVED,
        },
        SourceCorpusState.INDEXING: {
            SourceCorpusState.READY,
            SourceCorpusState.FAILED,
            SourceCorpusState.ARCHIVED,
        },
        SourceCorpusState.READY: {
            SourceCorpusState.INDEXING,
            SourceCorpusState.FAILED,
            SourceCorpusState.ARCHIVED,
        },
        SourceCorpusState.FAILED: {
            SourceCorpusState.INDEXING,
            SourceCorpusState.ARCHIVED,
        },
        SourceCorpusState.ARCHIVED: {
            SourceCorpusState.NEW,
        },
    }

    def __init__(self, repository: SourceCorpusRepository) -> None:
        self.repository = repository

    def handle(self, command: ManagementCommand) -> SourceCorpus:
        if isinstance(command, EnableSourceCorpusCommand):
            return self.enable(command)
        if isinstance(command, DisableSourceCorpusCommand):
            return self.disable(command)
        if isinstance(command, UpdateSourceCorpusParametersCommand):
            return self.update_parameters(command)
        if isinstance(command, ChangeSourceCorpusStateCommand):
            return self.change_state(command)
        raise TypeError(f"Unsupported command type: {type(command)}")

    def enable(self, command: EnableSourceCorpusCommand) -> SourceCorpus:
        corpus = self._get_or_raise(command.external_id)
        if corpus.is_enabled and corpus.state != SourceCorpusState.ARCHIVED:
            return corpus

        fields: dict[str, Any] = {"is_enabled": True}
        if corpus.state == SourceCorpusState.ARCHIVED:
            fields["state"] = SourceCorpusState.NEW
            fields["last_error"] = None

        updated = self.repository.update_corpus_fields(command.external_id, fields)
        if updated is None:
            raise SourceCorpusNotFoundError("Source corpus not found.")
        return updated

    def disable(self, command: DisableSourceCorpusCommand) -> SourceCorpus:
        corpus = self._get_or_raise(command.external_id)
        if not corpus.is_enabled and corpus.state == SourceCorpusState.ARCHIVED:
            return corpus

        fields: dict[str, Any] = {"is_enabled": False, "state": SourceCorpusState.ARCHIVED}
        if command.reason:
            fields["last_error"] = command.reason

        updated = self.repository.update_corpus_fields(command.external_id, fields)
        if updated is None:
            raise SourceCorpusNotFoundError("Source corpus not found.")
        return updated

    def update_parameters(self, command: UpdateSourceCorpusParametersCommand) -> SourceCorpus:
        corpus = self._get_or_raise(command.external_id)
        if corpus.state == SourceCorpusState.INDEXING and not command.force:
            raise InvalidSourceCorpusStateTransitionError(
                "Cannot update parameters while corpus is indexing without force flag."
            )

        if command.merge:
            new_params = dict(corpus.parameters)
            new_params.update(command.parameters)
        else:
            new_params = dict(command.parameters)

        updated = self.repository.update_corpus_fields(
            command.external_id,
            {"parameters": new_params},
        )
        if updated is None:
            raise SourceCorpusNotFoundError("Source corpus not found.")
        return updated

    def change_state(self, command: ChangeSourceCorpusStateCommand) -> SourceCorpus:
        corpus = self._get_or_raise(command.external_id)
        target = command.target_state
        if target == corpus.state:
            return corpus

        if not command.force and not self._can_transition(corpus.state, target):
            raise InvalidSourceCorpusStateTransitionError(
                f"Transition from {corpus.state.value} to {target.value} is not allowed."
            )

        if target == SourceCorpusState.INDEXING and not corpus.is_enabled:
            raise SourceCorpusDisabledError("Disabled corpus cannot be moved to indexing state.")

        fields: dict[str, Any] = {"state": target}

        if target == SourceCorpusState.FAILED:
            fields["last_error"] = command.reason or corpus.last_error or "Source corpus failed."
        elif target == SourceCorpusState.ARCHIVED:
            fields["is_enabled"] = False
            if command.reason:
                fields["last_error"] = command.reason
        else:
            fields["last_error"] = None

        if target == SourceCorpusState.READY:
            fields["indexed_at"] = _utc_timestamp()
        elif target == SourceCorpusState.INDEXING:
            fields["indexed_at"] = None

        updated = self.repository.update_corpus_fields(command.external_id, fields)
        if updated is None:
            raise SourceCorpusNotFoundError("Source corpus not found.")
        return updated

    def _get_or_raise(self, external_id: str) -> SourceCorpus:
        corpus = self.repository.get_corpus_by_external_id(external_id)
        if corpus is None:
            raise SourceCorpusNotFoundError("Source corpus not found.")
        return corpus

    @classmethod
    def _can_transition(cls, source: SourceCorpusState, target: SourceCorpusState) -> bool:
        return target in cls._ALLOWED_TRANSITIONS.get(source, set())


def _utc_timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()
