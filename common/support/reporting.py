from dataclasses import dataclass
from typing import Protocol


class Reporter(Protocol):
    def info(self, msg: str) -> None: ...


@dataclass(frozen=True, slots=True)
class PrintReporter:
    def info(self, msg: str) -> None:
        print(msg)


@dataclass(frozen=True, slots=True)
class NullReporter:
    def info(self, msg: str) -> None:
        return
