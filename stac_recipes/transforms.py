from dataclasses import dataclass

import apache_beam as beam


@dataclass
class Branch(beam.PTransform):
    transforms: list[beam.PTransform]

    def expand(self, pcoll):
        for transform in self.transforms:
            yield pcoll | transform


@dataclass
class Passthrough(beam.PTransform):
    def expand(self, pcoll):
        return pcoll
