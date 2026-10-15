"""
Bye outlook: for each week of a horizon, the best lineup a roster can field, which of its
players are on an NFL bye, and whether those byes leave a hole worth fixing (a "crunch").

Recommenders use it to look ahead instead of reacting to one week: waivers flag adds
that cover a crunch week, the trade context hands both teams' crunch weeks to the
proposal/negotiation LLMs, and the lineup page warns before the week arrives. It is
evidence for a move, never a reason on its own to drop or trade a good player - the move
itself is still judged by roster_move_scoring over the whole horizon.

Read-only; built from the same live trade context (ESPN rosters + exact-week
weekly_stats) as the trade score, so both always agree on who is on bye.
"""
from src.nfl_byes import bye_week_for, bye_weeks_or_empty, teams_playing_from_byes
from src.optimization.bye_outlook import build_bye_outlook, horizon_weeks
from src.services.trade_context_service import get_league_trade_context


def get_bye_outlook(league_id: str, season: int, week: int, horizon: int, team_id: str) -> dict:
    """One team's bye outlook from the shared trade context. Raises ValueError for an
    unknown team; trade-context failures propagate unchanged."""
    context = get_league_trade_context(league_id, season, week, horizon)
    if team_id not in context["teams"]:
        raise ValueError(f"Unknown ESPN team {team_id} in this league.")
    team = context["teams"][team_id]
    bye_weeks = bye_weeks_or_empty(season)
    roster = [{**player, "nfl_bye_week": bye_week_for(player.get("nfl_team"), bye_weeks)}
              for player in team["roster"]]
    weeks = horizon_weeks(context)
    result = build_bye_outlook(
        roster, weeks, context["as_of_week"], context["lineup_slot_counts"],
        {w: teams_playing_from_byes(bye_weeks, w) for w in weeks},
    )
    return {
        "team_id": team_id,
        "team_name": team.get("name"),
        "season": season,
        "start_week": context["start_week"],
        "end_week": context["end_week"],
        "schedule_known": bool(bye_weeks),
        **result,
        "players": [
            {key: player.get(key) for key in ("espn_id", "name", "position", "nfl_team", "nfl_bye_week")}
            for player in roster
        ],
    }
