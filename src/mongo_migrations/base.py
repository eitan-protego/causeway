"""MigrationStep base class."""

import re
from abc import ABC, abstractmethod
from typing import Any, ClassVar

from pymongo.asynchronous.database import AsyncDatabase


def class_name_to_words(name: str) -> str:
    """Convert CamelCase to lowercase words: AddCaseIndexes -> 'add case indexes'."""
    words = re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", " ", name)
    return words.lower()


class MigrationStep(ABC):
    """Base class for all migration steps.

    The runner discovers subclasses by inspecting loaded migration modules
    and assigns version and step numbers based on filename and definition order.

    Override up() (required) and optionally down() for rollback support.
    """

    version: ClassVar[int]
    step: ClassVar[int]
    name: ClassVar[str]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls.name = class_name_to_words(cls.__name__)

    @abstractmethod
    async def up(self, db: AsyncDatabase[dict[str, Any]]) -> None:
        """Apply this migration step."""

    async def down(
        self,
        db: AsyncDatabase[dict[str, Any]],  # pyright: ignore[reportUnusedParameter]
    ) -> None:
        """Reverse this migration step. Override to support rollback."""
        raise NotImplementedError(
            f"Migration '{self.name}' (v{self.version} step {self.step}) is irreversible"
        )

    def has_down(self) -> bool:
        """Whether this step has a real down() implementation."""
        return type(self).down is not MigrationStep.down
