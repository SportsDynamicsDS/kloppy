from datetime import datetime, timedelta, timezone
import json
from typing import IO, List, Tuple

from kloppy.domain import Ground, Period, Player, Score, Team

from .base import SecondSpectrumParser

DEFAULT_SECONDSPECTRUM_FRAME_RATE = 25.0   # fps


class MetadataJSONParser(SecondSpectrumParser):
    """Extract data from a Second Spectrum metadata file in JSON format."""

    def __init__(self, feed: IO[bytes]) -> None:
        self.feed = json.loads(feed.read().decode('utf-8'))

    def extract_date(self) -> datetime:
        """Return the date of the game."""
        return datetime(
            self.feed["year"],
            self.feed["month"],
            self.feed["day"],
            0,
            0,
            tzinfo=timezone.utc,
        )

    def extract_frame_rate(self) -> int:
        """Return the frame rate of the game."""
        return int(self.feed.get("fps", DEFAULT_SECONDSPECTRUM_FRAME_RATE))

    def extract_game_id(self) -> str:
        """Return the game_id of the game."""
        return self.feed["ssiId"]

    def extract_lineups(self) -> Tuple[Team, Team]:
        """Return the home and away team lineups."""
        # Default team initialisation
        teams = [
            Team(team_id="home", name="home", ground=Ground.HOME),
            Team(team_id="away", name="away", ground=Ground.AWAY),
        ]

        # Tries to parse (short) team names from the description string
        try:
            home_name = (
                self.feed["description"].split("-")[0].strip()
            )
            away_name = (
                self.feed["description"]
                .split("-")[1]
                .split(":")[0]
                .strip()
            )
        except (KeyError, IndexError, AttributeError):
            home_name, away_name = "home", "away"

        team_info = [
            {
                "team_id": self.feed["homeOptaId"],
                "name": home_name,
                "players_key": "homePlayers"
            },
            {
                "team_id": self.feed["awayOptaId"],
                "name": away_name,
                "players_key": "awayPlayers"
            },
        ]

        for team, info in zip(teams, team_info):
            team.team_id = info["team_id"]
            team.name = info["name"]

            for player_data in self.feed[info["players_key"]]:
                player_attributes = {
                    k: v
                    for k, v in player_data.items()
                    if k in ["ssiId", "optaUuid"]
                }

                player = Player(
                    player_id=player_data["optaId"],
                    name=player_data["name"],
                    starting=player_data["position"] != "SUB",
                    starting_position=player_data["position"],
                    team=team,
                    jersey_no=int(player_data["number"]),
                    attributes=player_attributes,
                )
                team.players.append(player)

        return teams

    def extract_periods(self) -> List[Period]:
        """Return the periods of the game."""
        periods = []
        for period in self.feed["periods"]:
            start_frame_id = int(period["startFrameIdx"])
            end_frame_id = int(period["endFrameIdx"])
            if start_frame_id != 0 or end_frame_id != 0:
                # Frame IDs are unix timestamps (in milliseconds)
                periods.append(
                    Period(
                        id=int(period["number"]),
                        start_timestamp=timedelta(
                            seconds=start_frame_id / self.extract_frame_rate()
                        ),
                        end_timestamp=timedelta(
                            seconds=end_frame_id / self.extract_frame_rate()
                        ),
                    )
                )
        return periods

    def extract_pitch_dimensions(self) -> Tuple[float, float]:
        """Return the pitch dimensions."""
        return self.feed["pitchLength"], self.feed["pitchWidth"]

    def extract_score(self) -> Score:
        """Return the score of the game."""
        return Score(
            home=self.feed["homeScore"],
            away=self.feed["awayScore"],
        )
