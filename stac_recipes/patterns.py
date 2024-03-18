from dataclasses import dataclass
from typing import Callable, Sequence

from pangeo_forge_recipes.patterns import FileType
from tlz.itertoolz import identity


@dataclass
class FilePattern:
    path_fn: Callable
    sequence: Sequence
    file_type: FileType | str
    key_formatter: Callable | str = identity

    def __post_init__(self):
        if isinstance(self.file_type, str):
            self.file_type = FileType(self.file_type)
        if isinstance(self.key_formatter, str):
            formatters = {
                "isoformat": lambda ts: ts.isoformat(),
            }
            formatter = formatters.get(self.key_formatter)
            if formatter is None:
                raise ValueError(f"unknown key formatter: {self.key_formatter}")

            self.key_formatter = formatter

    @classmethod
    def from_sequence(cls, sequence, file_type, **kwargs):
        return cls(identity, sequence, file_type, **kwargs)

    def items(self):
        for item in self.sequence:
            yield self.key_formatter(item), self.path_fn(item)
