from unittest.mock import MagicMock

import pytest

from src import projection_consensus as pc


@pytest.mark.parametrize(
    "espn, sleeper, expected",
    [
        (None, 15.0, 15.0),   # ESPN not synced yet: Sleeper alone, never null or 0
        (18.0, None, 18.0),   # Sleeper missing: ESPN alone
        (18.0, 15.0, 16.5),   # both: mean
        (0.0, 10.0, 5.0),     # a real 0.0 projection is a source, not a missing one
        (None, None, None),
    ],
)
def test_consensus_projection(espn, sleeper, expected):
    assert pc.consensus_projection(espn, sleeper) == expected


def test_recompute_touches_only_rows_with_a_source():
    cursor = MagicMock()
    pc.recompute_consensus(cursor, 2026, 13, ["b", "a", "a"])

    query, params = cursor.execute.call_args.args
    assert "SET projected_points" in query
    assert "espn_projected_points IS NOT NULL OR sleeper_projected_points IS NOT NULL" in query
    assert params == (["a", "b"], 2026, 13)


def test_recompute_noop_without_players():
    cursor = MagicMock()
    assert pc.recompute_consensus(cursor, 2026, 13, []) == 0
    cursor.execute.assert_not_called()
