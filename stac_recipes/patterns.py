from dataclasses import dataclass

import pandas as pd
from pangeo_forge_recipes.patterns import FileType


@dataclass
class FilePattern:
    path_fn: callable
    time: pd.Series
    file_type: FileType

    def items(self):
        for ts in self.time:
            yield ts, self.path_fn(ts)
