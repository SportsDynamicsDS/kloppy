"""Convert SecondSpectrum event stream data to a kloppy EventDataset."""

from .deserializer import (
    SecondSpectrumEventDataDeserializer,
    SecondSpectrumEventDataInputs,
)


__all__ = [
    "SecondSpectrumEventDataDeserializer",
    "SecondSpectrumEventDataInputs",
]
