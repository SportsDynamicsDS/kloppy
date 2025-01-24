from datetime import datetime, timedelta
import json
import logging
import pytz
from typing import Dict, IO, List, Tuple

from kloppy.domain import (
    BallState,
    DatasetTransformer,
    Event,
    Period,
    Point,
    Team,
)
from kloppy.exceptions import DeserializationError
from kloppy.infra.serializers.event.deserializer import EventDataDeserializer
from kloppy.infra.serializers.event.statsperform.deserializer import (
    BALL_OUT_EVENTS,
    BALL_OWNING_EVENTS,
    DEAD_BALL_EVENTS,
    DUEL_EVENTS,
    EVENT_TYPE_BALL_TOUCH,
    EVENT_TYPE_BLOCKED_PASS,
    EVENT_TYPE_CARD, EVENT_TYPE_CLEARANCE,
    EVENT_TYPE_END_PERIOD,
    EVENT_TYPE_FORMATION_CHANGE,
    EVENT_TYPE_FOUL_COMMITTED,
    EVENT_TYPE_INTERCEPTION,
    EVENT_TYPE_OFFSIDE_PASS,
    EVENT_TYPE_PASS,
    EVENT_TYPE_PLAYER_OFF,
    EVENT_TYPE_PLAYER_ON,
    EVENT_TYPE_RECOVERY,
    EVENT_TYPE_SHOT_GOAL,
    EVENT_TYPE_SHOT_MISS,
    EVENT_TYPE_SHOT_POST,
    EVENT_TYPE_SHOT_SAVED,
    EVENT_TYPE_START_PERIOD,
    EVENT_TYPE_TAKE_ON,
    KEEPER_EVENTS,
    position_line_mapping,
    _get_event_type_name,
    _parse_card,
    _parse_clearance,
    _parse_duel,
    _parse_formation_change,
    _parse_goalkeeper_events,
    _parse_interception,
    _parse_offside_pass,
    _parse_pass,
    _parse_shot,
    _parse_take_on,
)
from kloppy.infra.serializers.event.statsperform.parsers.base import OptaEvent

from .base import SecondSpectrumParser

logger = logging.getLogger(__name__)


def _parse_insight_datetime(dt_str: str) -> datetime:
    def zero_pad_milliseconds(timestamp: str) -> str:
        """Ensures milliseconds are zero-padded to 3 digits."""
        parts = timestamp.split(".")
        if len(parts) == 1:
            return timestamp + ".000"
        return ".".join(parts[:-1] + [f"{int(parts[-1]):03d}"])

    dt_str = zero_pad_milliseconds(dt_str)
    naive_datetime = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%f")
    timezone = pytz.timezone("Europe/London")
    aware_datetime = timezone.localize(naive_datetime)
    return aware_datetime.astimezone(pytz.utc)


def _parse_nearby_event(
        raw_events: List[OptaEvent],
        idx: int,
        offset: int = 1,
        ) -> OptaEvent:
    return (
        raw_events[idx + offset]
        if (idx + offset) < len(raw_events)
        else None
    )


def _parse_period(
        raw_event: OptaEvent,
        periods: Tuple[Team, Team],
        ) -> List[Period]:
    return next(
        (
            period
            for period in periods
            if period.id == raw_event.period_id
        ),
        None,
    )


def _parse_substitution(
        current_event: OptaEvent,
        previous_event: OptaEvent,
        next_event: OptaEvent,
        team: Team,
        ) -> Dict:
    related_event_id = int(current_event.qualifiers.get(55))
    for event in (previous_event, next_event):
        if (
            event.type_id == EVENT_TYPE_PLAYER_ON and
            event.event_id == related_event_id
        ):
            related_event = event
            replacement_player = team.get_player_by_id(event.player_id)
            break
    else:
        raise DeserializationError(
            f"Unable to determine replacement player for event_id {current_event.event_id}"
        )

    raw_position_line = related_event.qualifiers.get(44)
    if raw_position_line:
        position = position_line_mapping[raw_position_line]

    return dict(replacement_player=replacement_player, position=position)


def _parse_team(raw_event: OptaEvent, teams: Tuple[Team, Team]) -> Team:
    if raw_event.contestant_id == teams[0].team_id:
        team = teams[0]
    elif raw_event.contestant_id == teams[1].team_id:
        team = teams[1]
    else:
        raise DeserializationError(
            f"Unknown team_id {raw_event.contestant_id}"
        )
    return team


class InsightParser(SecondSpectrumParser):
    """Extract data from a Second Spectrum Insight data stream."""

    def __init__(self, feed: IO[bytes]) -> None:
        self.feed = [json.loads(line.decode("utf-8").strip()) for line in feed]

    def extract_events(self) -> List[OptaEvent]:
        """Return raw events."""
        return [
            OptaEvent(
                id=obj["optaEvent"]["id"],
                event_id=int(obj["optaEvent"]["eventId"]),
                type_id=int(obj["optaEvent"]["typeId"]),
                period_id=int(obj["optaEvent"]["periodId"]),
                time_min=int(obj["optaEvent"]["alignedClock"] // 60)
                if obj["optaEvent"]["alignedClock"] is not None else 0,
                time_sec=int(obj["optaEvent"]["alignedClock"] % 60)
                if obj["optaEvent"]["alignedClock"] is not None else 0,
                x=float(obj["optaEvent"]["x"]),
                y=float(obj["optaEvent"]["y"]),
                timestamp=_parse_insight_datetime(obj["optaEvent"]["timeStamp"]),
                last_modified=_parse_insight_datetime(obj["optaEvent"]["lastModified"]),
                contestant_id=str(obj["optaEvent"].get("opContestantId")),
                player_id=str(obj["optaEvent"].get("opPlayerId")),
                outcome=int(obj["optaEvent"].get("outcome")),
                qualifiers={
                    item["qualifierId"]: item.get("opValue", item.get("value"))
                    for item in obj["optaEvent"]["qualifier"]
                    if "qualifierId" in item
                },
            )
            for obj in self.feed
            if "optaEvent" in obj and isinstance(obj["optaEvent"], dict)
        ]

    def parse_events(
            self,
            deserializer: EventDataDeserializer,
            raw_events: List[OptaEvent],
            teams: Tuple[Team, Team],
            periods: List[Period],
            transformer: DatasetTransformer,
            ) -> List[Event]:
        """Return events."""
        possession_team = None
        events = []
        for idx, raw_event in enumerate(raw_events):
            team = _parse_team(raw_event, teams)
            period = _parse_period(raw_event, periods)
            previous_event = _parse_nearby_event(raw_events, idx, offset=-1)
            next_event = _parse_nearby_event(raw_events, idx, offset=1)
            next_next_event = _parse_nearby_event(raw_events, idx, offset=2)

            if period is None:
                logger.debug(
                    f"Skipping event {raw_event.id} because period doesn't match {raw_event.period_id}"
                )
                continue

            if raw_event.type_id == EVENT_TYPE_START_PERIOD:
                logger.debug(
                    f"Set start of period {period.id} to {raw_event.timestamp}"
                )
                period.start_timestamp = raw_event.timestamp
            elif raw_event.type_id == EVENT_TYPE_END_PERIOD:
                logger.debug(
                    f"Set end of period {period.id} to {raw_event.timestamp}"
                )
                period.end_timestamp = raw_event.timestamp
            elif raw_event.type_id == EVENT_TYPE_PLAYER_ON:
                continue
            else:
                if not period.start_timestamp:
                    # not started yet
                    continue

                player = None
                if raw_event.player_id is not None:
                    player = team.get_player_by_id(raw_event.player_id)

                if raw_event.type_id in BALL_OWNING_EVENTS:
                    possession_team = team

                if raw_event.type_id in DEAD_BALL_EVENTS:
                    ball_state = BallState.DEAD
                else:
                    ball_state = BallState.ALIVE

                generic_event_kwargs = dict(
                    # from DataRecord
                    period=period,
                    timestamp=raw_event.timestamp - period.start_timestamp,
                    ball_owning_team=possession_team,
                    ball_state=ball_state,
                    # from Event
                    event_id=raw_event.id,
                    team=team,
                    player=player,
                    coordinates=Point(x=raw_event.x, y=raw_event.y),
                    raw_event=raw_event,
                )

                if raw_event.type_id == EVENT_TYPE_PASS:
                    pass_event_kwargs = _parse_pass(
                        raw_event, next_event, next_next_event
                    )
                    event = deserializer.event_factory.build_pass(
                        **pass_event_kwargs,
                        **generic_event_kwargs,
                    )
                elif raw_event.type_id == EVENT_TYPE_OFFSIDE_PASS:
                    pass_event_kwargs = _parse_offside_pass(raw_event)
                    event = deserializer.event_factory.build_pass(
                        **pass_event_kwargs,
                        **generic_event_kwargs,
                    )
                elif raw_event.type_id == EVENT_TYPE_TAKE_ON:
                    take_on_event_kwargs = _parse_take_on(raw_event)
                    event = deserializer.event_factory.build_take_on(
                        **take_on_event_kwargs,
                        **generic_event_kwargs,
                        qualifiers=None,
                    )
                elif raw_event.type_id in (
                    EVENT_TYPE_SHOT_MISS,
                    EVENT_TYPE_SHOT_POST,
                    EVENT_TYPE_SHOT_SAVED,
                    EVENT_TYPE_SHOT_GOAL,
                ):
                    if raw_event.type_id == EVENT_TYPE_SHOT_GOAL:
                        if 374 in raw_event.qualifiers:
                            # Qualifier 374 specifies the actual time of the shot for all goal events
                            # It uses London timezone for both MA3 and F24 feeds
                            naive_datetime = datetime.strptime(
                                raw_event.qualifiers[374],
                                "%Y-%m-%d %H:%M:%S.%f",
                            )
                            timezone = pytz.timezone("Europe/London")
                            aware_datetime = timezone.localize(
                                naive_datetime
                            )
                            generic_event_kwargs["timestamp"] = (
                                aware_datetime.astimezone(pytz.utc)
                                - period.start_timestamp
                            )
                    shot_event_kwargs = _parse_shot(raw_event)
                    kwargs = {}
                    kwargs.update(generic_event_kwargs)
                    kwargs.update(shot_event_kwargs)
                    event = deserializer.event_factory.build_shot(**kwargs)
                elif raw_event.type_id == EVENT_TYPE_RECOVERY:
                    event = deserializer.event_factory.build_recovery(
                        result=None,
                        qualifiers=None,
                        **generic_event_kwargs,
                    )
                elif raw_event.type_id == EVENT_TYPE_CLEARANCE:
                    clearance_event_kwargs = _parse_clearance(raw_event)
                    event = deserializer.event_factory.build_clearance(
                        result=None,
                        **clearance_event_kwargs,
                        **generic_event_kwargs,
                    )
                elif raw_event.type_id in DUEL_EVENTS:
                    duel_event_kwargs = _parse_duel(raw_event)
                    event = deserializer.event_factory.build_duel(
                        **duel_event_kwargs,
                        **generic_event_kwargs,
                    )
                elif raw_event.type_id in (
                    EVENT_TYPE_INTERCEPTION,
                    EVENT_TYPE_BLOCKED_PASS,
                ):
                    interception_event_kwargs = _parse_interception(
                        raw_event, team, next_event
                    )
                    event = deserializer.event_factory.build_interception(
                        **interception_event_kwargs,
                        **generic_event_kwargs,
                    )
                elif raw_event.type_id in KEEPER_EVENTS:
                    # Qualifier 94 means the "save" event is a shot block by a defender
                    if 94 in raw_event.qualifiers:
                        event = deserializer.event_factory.build_generic(
                            **generic_event_kwargs,
                            result=None,
                            qualifiers=None,
                            event_name="block",
                        )
                    else:
                        goalkeeper_event_kwargs = _parse_goalkeeper_events(
                            raw_event
                        )
                        event = deserializer.event_factory.build_goalkeeper_event(
                            **goalkeeper_event_kwargs,
                            **generic_event_kwargs,
                        )
                elif (raw_event.type_id == EVENT_TYPE_BALL_TOUCH) & (
                    raw_event.outcome == 0
                ):
                    event = deserializer.event_factory.build_miscontrol(
                        result=None,
                        qualifiers=None,
                        **generic_event_kwargs,
                    )
                elif (raw_event.type_id == EVENT_TYPE_FOUL_COMMITTED) and (
                    raw_event.outcome == 0
                ):
                    event = deserializer.event_factory.build_foul_committed(
                        result=None,
                        qualifiers=None,
                        **generic_event_kwargs,
                    )
                elif raw_event.type_id in BALL_OUT_EVENTS:
                    event = deserializer.event_factory.build_ball_out(
                        result=None,
                        qualifiers=None,
                        **generic_event_kwargs,
                    )
                elif raw_event.type_id == EVENT_TYPE_FORMATION_CHANGE:
                    generic_event_kwargs["timestamp"] = max(
                        timedelta(0), generic_event_kwargs["timestamp"]
                    )
                    formation_change_event_kwargs = (
                        _parse_formation_change(raw_event, team)
                    )
                    event = deserializer.event_factory.build_formation_change(
                        result=None,
                        qualifiers=None,
                        **formation_change_event_kwargs,
                        **generic_event_kwargs,
                    )
                elif raw_event.type_id == EVENT_TYPE_PLAYER_OFF:
                    generic_event_kwargs["timestamp"] = max(
                        timedelta(0), generic_event_kwargs["timestamp"]
                    )
                    substitution_event_kwargs = _parse_substitution(
                        current_event=raw_event,
                        previous_event=previous_event,
                        next_event=next_event,
                        team=team,
                    )
                    event = deserializer.event_factory.build_substitution(
                        result=None,
                        qualifiers=None,
                        **substitution_event_kwargs,
                        **generic_event_kwargs,
                    )

                elif raw_event.type_id == EVENT_TYPE_CARD:
                    card_event_kwargs = _parse_card(raw_event)

                    event = deserializer.event_factory.build_card(
                        **card_event_kwargs,
                        **generic_event_kwargs,
                    )
                else:
                    event = deserializer.event_factory.build_generic(
                        **generic_event_kwargs,
                        result=None,
                        qualifiers=None,
                        event_name=_get_event_type_name(raw_event.type_id),
                    )

                if deserializer.should_include_event(event):
                    events.append(transformer.transform_event(event))

        return events
