"""Base class for all Second Spectrum event stream parsers.

A parser reads a single data file and should extend the 'SecondSpectrumParser' class to
extract data about players, teams and events that is encoded in the file.
"""

from datetime import datetime
from typing import Any, IO, List, Optional, Tuple

from kloppy.domain import Event, Period, Score, Team
from kloppy.domain.services.transformers.dataset import DatasetTransformer
from kloppy.infra.serializers.event.deserializer import EventDataDeserializer


class SecondSpectrumParser:
    """Extract data from an Second Spectrum data stream.

    Args:
        feed : The data stream of a game to parse.
    """

    def __init__(self, feed: IO[bytes]) -> None:
        raise NotImplementedError

    def extract_date(self) -> datetime:
        """Return the date of the game."""
        raise NotImplementedError

    def extract_events(self) -> List[Any]:
        """Return raw events."""
        raise NotImplementedError

    def extract_frame_rate(self) -> int:
        """Return the frame rate of the game."""
        raise NotImplementedError

    def extract_game_id(self) -> str:
        """Return the game_id of the game."""
        raise NotImplementedError

    def extract_game_week(self) -> Optional[str]:
        """Return the game_week of the game."""
        return None

    def extract_lineups(self) -> Tuple[Team, Team]:
        """Return the home and away team lineups."""
        raise NotImplementedError

    def extract_periods(self) -> List[Period]:
        """Return the periods of the game."""
        raise NotImplementedError

    def extract_pitch_dimensions(self) -> Tuple[float, float]:
        """Return the pitch dimensions."""
        raise NotImplementedError

    def extract_score(self) -> Score:
        """Return the score of the game."""
        raise NotImplementedError

    def parse_events(
            self,
            deserializer: EventDataDeserializer,
            raw_events: List[Any],
            teams: Tuple[Team, Team],
            periods: List[Period],
            transformer: DatasetTransformer,
            ) -> List[Event]:
        """Return events."""
        raise NotImplementedError
