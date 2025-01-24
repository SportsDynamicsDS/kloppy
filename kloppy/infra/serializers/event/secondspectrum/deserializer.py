from typing import NamedTuple, IO, Optional
import logging

from kloppy.domain import (
    EventDataset,
    DatasetFlag,
    Orientation,
    Provider,
    Metadata,
)
from kloppy.infra.serializers.event.deserializer import EventDataDeserializer
from kloppy.infra.serializers.event.statsperform.deserializer import (
    EVENT_TYPE_DELETED_EVENT,
)
from kloppy.utils import performance_logging

from .parsers import get_event_parser, get_metadata_parser


logger = logging.getLogger(__name__)


class SecondSpectrumEventDataInputs(NamedTuple):
    meta_data: IO[bytes]
    event_data: IO[bytes]
    meta_datatype: Optional[str] = None
    event_feed: Optional[str] = None


class SecondSpectrumEventDataDeserializer(
    EventDataDeserializer[SecondSpectrumEventDataInputs]
):
    @property
    def provider(self) -> Provider:
        return Provider.SECONDSPECTRUM

    def deserialize(
            self,
            inputs: SecondSpectrumEventDataInputs
    ) -> EventDataset:
        with performance_logging("load data", logger=logger):
            metadata_parser = get_metadata_parser(
                inputs.meta_data,
                inputs.meta_datatype,
            )
            events_parser = get_event_parser(
                inputs.event_data,
                inputs.event_feed,
            )

        with performance_logging("parse data", logger=logger):
            date = metadata_parser.extract_date()
            frame_rate = metadata_parser.extract_frame_rate()
            game_id = metadata_parser.extract_game_id()
            periods = metadata_parser.extract_periods()
            pitch_length, pitch_width = metadata_parser.extract_pitch_dimensions()
            score = metadata_parser.extract_score()
            teams = metadata_parser.extract_lineups()
            raw_events = [
                event
                for event in events_parser.extract_events()
                if event.type_id != EVENT_TYPE_DELETED_EVENT
            ]

            transformer = self.get_transformer(
                pitch_length=pitch_length,
                pitch_width=pitch_width,
            )

            events = events_parser.parse_events(
                deserializer=self,
                raw_events=raw_events,
                teams=teams,
                periods=periods,
                transformer=transformer,
            )

        metadata = Metadata(
            teams=list(teams),
            periods=periods,
            pitch_dimensions=transformer.get_to_coordinate_system().pitch_dimensions,
            score=score,
            frame_rate=frame_rate,
            orientation=Orientation.ACTION_EXECUTING_TEAM,
            flags=DatasetFlag.BALL_OWNING_TEAM | DatasetFlag.BALL_STATE,
            provider=Provider.SECONDSPECTRUM,
            coordinate_system=transformer.get_to_coordinate_system(),
            date=date,
            game_week=None,   # Not available
            game_id=game_id,
        )

        return EventDataset(
            metadata=metadata,
            records=events,
        )
