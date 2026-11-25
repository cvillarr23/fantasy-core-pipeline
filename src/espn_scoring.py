"""
ESPN fantasy scoring, explained: the one map from ESPN's fantasy stat ids to plain-English labels,
the league rules built from `league_settings.scoring_settings` (`GET /v1/scoring/rules`), and the
per-player breakdown on the live pages.

Points are never recomputed here. A breakdown line's `points` is ESPN's own `appliedStats` value
for that stat, so the lines add up to ESPN's `appliedTotal` (`live_points`). The league rule only
supplies the `rule_label` text. When the lines don't add up (a stat id missing from `ESPN_STATS`),
an `other` line carries the remainder instead of hiding it.

`scoringItems` entries are `{statId, points, pointsOverrides}`. `pointsOverrides` is keyed by
lineup slot id and replaces `points` for players in that position: this league scores every D/ST
stat that way (`points: 0`, `pointsOverrides: {"16": 2.0}`).
"""
import logging
from dataclasses import dataclass

logger = logging.getLogger("EspnScoring")

PER, BUCKET = "per", "bucket"
OTHER_KEY = "other"
SUM_TOLERANCE = 0.05
MINUS = "−"

# Lineup slot ids that `pointsOverrides` is keyed by.
OVERRIDE_SLOT_POSITIONS = {"0": "QB", "2": "RB", "4": "WR", "6": "TE", "16": "D/ST", "17": "K"}


@dataclass(frozen=True)
class StatDef:
    stat_key: str
    label: str
    # A `per` stat's rule reads "<pts> per <noun>"; a `bucket` stat's reads "<pts> for <noun>".
    noun: str
    unit: str | None
    category: str
    kind: str = PER
    # Yard stats: "1 pt per 10 rushing yds" reads better than "0.1 pts per rushing yd".
    per_many: str | None = None


def _yds(key, label, noun, category):
    return StatDef(key, label, f"{noun} yd", "yds", category, per_many=f"{noun} yds")


def _bucket(key, label, noun):
    return StatDef(key, label, noun, None, "Defense", BUCKET)


# ESPN fantasy stat id -> definition, in display order. Keys reuse `live_games.stat_line`'s names
# where the two overlap. Ids from espn_api's PLAYER_STATS_MAP; 198 and 209 aren't in it and were
# read off this league's settings and live payloads.
ESPN_STATS = {
    3: _yds("passYd", "Passing yards", "passing", "Passing"),
    4: StatDef("passTD", "Passing TD", "passing TD", "TD", "Passing"),
    19: StatDef("passTwoPt", "Passing 2-pt conversion", "passing 2-pt conversion", "2PT", "Passing"),
    20: StatDef("int", "Interception thrown", "interception thrown", "INT", "Passing"),
    24: _yds("rushYd", "Rushing yards", "rushing", "Rushing"),
    25: StatDef("rushTD", "Rushing TD", "rushing TD", "TD", "Rushing"),
    26: StatDef("rushTwoPt", "Rushing 2-pt conversion", "rushing 2-pt conversion", "2PT", "Rushing"),
    53: StatDef("rec", "Receptions", "reception", "rec", "Receiving"),
    42: _yds("recYd", "Receiving yards", "receiving", "Receiving"),
    43: StatDef("recTD", "Receiving TD", "receiving TD", "TD", "Receiving"),
    44: StatDef("recTwoPt", "Receiving 2-pt conversion", "receiving 2-pt conversion", "2PT", "Receiving"),
    72: StatDef("fumLost", "Fumble lost", "fumble lost", "lost", "Turnovers"),
    63: StatDef("fumRecTD", "Fumble recovered for TD", "fumble recovered for a TD", "TD", "Turnovers"),
    83: StatDef("fgMade", "Field goal made", "field goal made", "FG", "Kicking"),
    80: StatDef("fgUnder40", "FG made (under 40 yds)", "FG made from under 40 yds", "FG", "Kicking"),
    77: StatDef("fg40To49", "FG made (40–49 yds)", "FG made from 40–49 yds", "FG", "Kicking"),
    74: StatDef("fg50Plus", "FG made (50+ yds)", "FG made from 50+ yds", "FG", "Kicking"),
    198: StatDef("fg50To59", "FG made (50–59 yds)", "FG made from 50–59 yds", "FG", "Kicking"),
    201: StatDef("fg60Plus", "FG made (60+ yds)", "FG made from 60+ yds", "FG", "Kicking"),
    85: StatDef("fgMissed", "Field goal missed", "missed field goal", "FG", "Kicking"),
    86: StatDef("xpMade", "Extra point made", "extra point made", "XP", "Kicking"),
    88: StatDef("xpMissed", "Extra point missed", "missed extra point", "XP", "Kicking"),
    99: StatDef("sacks", "Sacks", "sack", "sacks", "Defense"),
    95: StatDef("defInt", "Interceptions", "interception", "INT", "Defense"),
    96: StatDef("fumRec", "Fumbles recovered", "fumble recovered", "FR", "Defense"),
    98: StatDef("safeties", "Safeties", "safety", "safeties", "Defense"),
    97: StatDef("blockedKicks", "Blocked kicks", "blocked kick", "blocks", "Defense"),
    94: StatDef("defTD", "Defensive TD", "defensive TD", "TD", "Defense"),
    105: StatDef("defStTD", "Defense or special teams TD", "defense or special teams TD", "TD", "Defense"),
    93: StatDef("blockedKickTD", "Blocked kick returned for TD", "blocked kick returned for a TD", "TD", "Defense"),
    103: StatDef("intTD", "Interception returned for TD", "interception returned for a TD", "TD", "Defense"),
    104: StatDef("fumRetTD", "Fumble returned for TD", "fumble returned for a TD", "TD", "Defense"),
    205: StatDef("twoPtReturn", "2-pt conversion return", "2-pt conversion return", "2PT", "Defense"),
    206: StatDef("twoPtReturn", "2-pt conversion return", "2-pt conversion return", "2PT", "Defense"),
    209: StatDef("onePtSafety", "1-pt safety", "1-pt safety", "safeties", "Defense"),
    89: _bucket("ptsAllowed0", "0 points allowed", "allowing 0 points"),
    90: _bucket("ptsAllowed1To6", "1–6 points allowed", "allowing 1–6 points"),
    91: _bucket("ptsAllowed7To13", "7–13 points allowed", "allowing 7–13 points"),
    92: _bucket("ptsAllowed14To17", "14–17 points allowed", "allowing 14–17 points"),
    121: _bucket("ptsAllowed18To21", "18–21 points allowed", "allowing 18–21 points"),
    122: _bucket("ptsAllowed22To27", "22–27 points allowed", "allowing 22–27 points"),
    123: _bucket("ptsAllowed28To34", "28–34 points allowed", "allowing 28–34 points"),
    124: _bucket("ptsAllowed35To45", "35–45 points allowed", "allowing 35–45 points"),
    125: _bucket("ptsAllowed46Plus", "46+ points allowed", "allowing 46+ points"),
    128: _bucket("ydsAllowedUnder100", "Under 100 yds allowed", "allowing under 100 yds"),
    129: _bucket("ydsAllowed100To199", "100–199 yds allowed", "allowing 100–199 yds"),
    130: _bucket("ydsAllowed200To299", "200–299 yds allowed", "allowing 200–299 yds"),
    131: _bucket("ydsAllowed300To349", "300–349 yds allowed", "allowing 300–349 yds"),
    132: _bucket("ydsAllowed350To399", "350–399 yds allowed", "allowing 350–399 yds"),
    133: _bucket("ydsAllowed400To449", "400–449 yds allowed", "allowing 400–449 yds"),
    134: _bucket("ydsAllowed450To499", "450–499 yds allowed", "allowing 450–499 yds"),
    135: _bucket("ydsAllowed500To549", "500–549 yds allowed", "allowing 500–549 yds"),
    136: _bucket("ydsAllowed550Plus", "550+ yds allowed", "allowing 550+ yds"),
    101: StatDef("kickRetTD", "Kickoff returned for TD", "kickoff returned for a TD", "TD", "Returns"),
    102: StatDef("puntRetTD", "Punt returned for TD", "punt returned for a TD", "TD", "Returns"),
}
_DISPLAY_ORDER = {stat_id: index for index, stat_id in enumerate(ESPN_STATS)}


# ---------------------------------------------------------------- rule labels


def _number(value: float) -> str:
    text = f"{abs(value):.2f}".rstrip("0").rstrip(".")
    return f"{MINUS}{text}" if value < 0 else text


def _points(value: float) -> str:
    return f"{_number(value)} {'pt' if abs(value) == 1 else 'pts'}"


def _units_per_point(points: float) -> int | None:
    """10 for 0.1 pts per yard: a whole number of units per point, else None."""
    if not points or abs(points) >= 1:
        return None
    units = 1 / abs(points)
    return round(units) if abs(units - round(units)) < 1e-6 else None


def rule_label(stat: StatDef, points: float) -> str:
    """"1 pt per 10 rushing yds", "6 pts per receiving TD", "−2 pts per fumble lost",
    "5 pts for allowing 0 points"."""
    if stat.kind == BUCKET:
        return f"{_points(points)} for {stat.noun}"
    units = _units_per_point(points) if stat.per_many else None
    if units:
        return f"{_points(1 if points > 0 else -1)} per {units} {stat.per_many}"
    return f"{_points(points)} per {stat.noun}"


# ---------------------------------------------------------------- league rules


def _scoring_items(scoring_settings) -> list:
    return (scoring_settings or {}).get("scoringItems") or []


def _item_stat_id(item: dict):
    try:
        return int(item.get("statId"))
    except (TypeError, ValueError):
        return None


def _item_scopes(item: dict) -> list:
    """`(position or None, points)` per scope a scoring item applies to: everyone at `points`
    (when non-zero), plus each overridden position whose points differ."""
    base = float(item.get("points") or 0)
    scopes = [(None, base)] if base else []
    for slot, points in sorted((item.get("pointsOverrides") or {}).items()):
        position = OVERRIDE_SLOT_POSITIONS.get(str(slot))
        if position is not None and points is not None and float(points) != base:
            scopes.append((position, float(points)))
    return scopes


def rule_points(scoring_settings) -> dict:
    """`(stat id, position or None) -> points` for every scored stat, known to `ESPN_STATS` or
    not. `None` is the rule for everyone; a position key is a `pointsOverrides` entry."""
    index = {}
    for item in _scoring_items(scoring_settings):
        stat_id = _item_stat_id(item)
        if stat_id is None:
            continue
        for position, points in _item_scopes(item):
            index[(stat_id, position)] = points
    return index


def _rule(stat_id: int, position, points: float) -> dict:
    stat = ESPN_STATS[stat_id]
    return {
        "stat_key": stat.stat_key,
        "label": stat.label,
        "category": stat.category,
        "points_per": points,
        "unit": stat.unit,
        "rule_label": rule_label(stat, points),
        "position": position,
    }


def scoring_rules(scoring_settings) -> list:
    """The league's rules in display order, one per stat and position scope. Stat ids missing
    from `ESPN_STATS` are logged and skipped."""
    index = rule_points(scoring_settings)
    unknown = sorted({stat_id for stat_id, _ in index if stat_id not in ESPN_STATS})
    if unknown:
        logger.warning(f"Skipping scoring rules for unknown ESPN stat ids: {unknown}")
    known = [(key, points) for key, points in index.items() if key[0] in ESPN_STATS]
    known.sort(key=lambda entry: (_DISPLAY_ORDER[entry[0][0]], entry[0][1] or ""))
    return [_rule(stat_id, position, points) for (stat_id, position), points in known]


# ---------------------------------------------------------------- breakdown


def _qty(applied_qty: dict, stat_id: str):
    value = (applied_qty or {}).get(stat_id)
    return float(value) if value is not None else None


def _line_rule_label(stat: StatDef, stat_id: int, position, points: float, qty, rules: dict) -> str | None:
    """The league rule for this player's position, else everyone's; when neither exists, what
    ESPN's own points per unit imply."""
    per = rules.get((stat_id, position), rules.get((stat_id, None)))
    if per is None and qty:
        per = points / qty
    return rule_label(stat, per) if per is not None else None


def _breakdown_line(stat_id: int, points: float, qty, position, rules: dict) -> dict:
    stat = ESPN_STATS[stat_id]
    return {
        "stat_key": stat.stat_key,
        "label": stat.label,
        "qty": qty,
        "unit": stat.unit,
        "rule_label": _line_rule_label(stat, stat_id, position, points, qty, rules),
        "points": round(points, 2),
    }


def _other_line(remainder: float) -> dict:
    return {
        "stat_key": OTHER_KEY,
        "label": "Other scoring",
        "qty": None,
        "unit": None,
        "rule_label": "Not itemized",
        "points": round(remainder, 2),
    }


def _scored_stats(applied_stats: dict) -> list:
    """`(stat id, points)` for every non-zero applied stat, in display order. Unknown ids are
    logged and left to the `other` line."""
    scored, unknown = [], []
    for raw_id, points in (applied_stats or {}).items():
        stat_id = int(raw_id)
        if not points:
            continue
        if stat_id in ESPN_STATS:
            scored.append((stat_id, float(points)))
        else:
            unknown.append(stat_id)
    if unknown:
        logger.warning(f"Breakdown has unknown ESPN stat ids, counted as other: {sorted(unknown)}")
    return sorted(scored, key=lambda entry: _DISPLAY_ORDER[entry[0]])


def score_breakdown(live_player: dict, rules: dict) -> list | None:
    """`[{stat_key, label, qty, unit, rule_label, points}]` for one `live:player` value, given
    `rule_points(...)`. None when the player has no live points yet. The lines sum to
    `live_points` within 0.05, with an `other` line for any remainder."""
    live_points = live_player.get("live_points")
    if live_points is None:
        return None
    position = live_player.get("position")
    applied_qty = live_player.get("applied_qty") or {}
    lines = [
        _breakdown_line(stat_id, points, _qty(applied_qty, str(stat_id)), position, rules)
        for stat_id, points in _scored_stats(live_player.get("applied_stats"))
    ]
    remainder = float(live_points) - sum(line["points"] for line in lines)
    if abs(remainder) > SUM_TOLERANCE:
        lines.append(_other_line(remainder))
    return lines
