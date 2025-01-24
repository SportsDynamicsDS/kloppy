from typing import List, Optional

from kloppy.config import get_config
from kloppy.domain import EventDataset, EventFactory, TrackingDataset
from kloppy.infra.serializers.event.secondspectrum import (
    SecondSpectrumEventDataDeserializer,
    SecondSpectrumEventDataInputs,
)
from kloppy.infra.serializers.tracking.secondspectrum import (
    SecondSpectrumTrackingDataDeserializer,
    SecondSpectrumTrackingDataInputs,
)
from kloppy.io import FileLike, open_as_file, Source
from kloppy.utils import deprecated


def load_event(
    event_data: FileLike,
    meta_data: FileLike,
    event_types: Optional[List[str]] = None,
    coordinates: Optional[str] = None,
    event_factory: Optional[EventFactory] = None,
) -> EventDataset:
    """
    Load Second Spectrum event data into a [`EventDataset`][kloppy.domain.models.event.EventDataset]

    Parameters:
        event_data: filename of the JSON Lines file containing the events
        meta_data: filename of the JSON Lines file containing the match information
        event_types:
        coordinates:
        event_factory:

    """
    serializer = SecondSpectrumEventDataDeserializer(
        event_types=event_types,
        coordinate_system=coordinates,
        event_factory=event_factory or get_config("event_factory"),
    )
    with open_as_file(event_data) as event_data_fp, open_as_file(
        meta_data
    ) as meta_data_fp:
        return serializer.deserialize(
            SecondSpectrumEventDataInputs(
                meta_data=meta_data_fp,
                event_data=event_data_fp,
            )
        )


def load_tracking(
    meta_data: FileLike,
    raw_data: FileLike,
    additional_meta_data: Optional[FileLike] = None,
    sample_rate: Optional[float] = None,
    limit: Optional[int] = None,
    coordinates: Optional[str] = None,
    only_alive: Optional[bool] = False,
) -> TrackingDataset:
    deserializer = SecondSpectrumTrackingDataDeserializer(
        sample_rate=sample_rate,
        limit=limit,
        coordinate_system=coordinates,
        only_alive=only_alive,
    )
    with open_as_file(meta_data) as meta_data_fp, open_as_file(
        raw_data
    ) as raw_data_fp, open_as_file(
        Source.create(additional_meta_data, optional=True)
    ) as additional_meta_data_fp:
        return deserializer.deserialize(
            inputs=SecondSpectrumTrackingDataInputs(
                meta_data=meta_data_fp,
                raw_data=raw_data_fp,
                additional_meta_data=additional_meta_data_fp,
            )
        )


@deprecated("secondspectrum.load_tracking should be used")
def load(
    meta_data: FileLike,
    raw_data: FileLike,
    additional_meta_data: Optional[FileLike] = None,
    sample_rate: Optional[float] = None,
    limit: Optional[int] = None,
    coordinates: Optional[str] = None,
    only_alive: Optional[bool] = False,
) -> TrackingDataset:
    return load_tracking(
        meta_data,
        raw_data,
        additional_meta_data,
        sample_rate,
        limit,
        coordinates,
        only_alive,
    )
