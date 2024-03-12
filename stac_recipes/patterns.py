from dataclasses import dataclass

import pandas as pd
from pangeo_forge_recipes.patterns import FileType


@dataclass
class FilePattern:
    path_fn: callable
    time: pd.Series
    file_type: FileType | str

    def __post_init__(self):
        if isinstance(self.file_type, str):
            self.file_type = FileType(self.file_type)

    def items(self):
        for ts in self.time:
            yield ts, self.path_fn(ts)
