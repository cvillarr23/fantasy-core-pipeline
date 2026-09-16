# Declarative DDL statements establishing data metrics and time-series schemas

CREATE_PLAYERS_TABLE = """
CREATE TABLE IF NOT EXISTS players (
    player_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    position VARCHAR(10) NOT NULL,
    team VARCHAR(10) NOT NULL,
    status VARCHAR(20) NOT NULL,
    injury_status VARCHAR(20),
    gsis_id VARCHAR(20),
    espn_id VARCHAR(20),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# Backfills gsis_id on players tables created before this column existed.
ADD_PLAYERS_GSIS_ID_COLUMN = """
ALTER TABLE players ADD COLUMN IF NOT EXISTS gsis_id VARCHAR(20);
"""

# Backfills espn_id (CORE-05) on players tables created before this column existed.
# Populated by sync_id_crosswalk from the DynastyProcess crosswalk, and joined against
# ESPN roster payloads in sync_league to resolve league_rosters rows.
ADD_PLAYERS_ESPN_ID_COLUMN = """
ALTER TABLE players ADD COLUMN IF NOT EXISTS espn_id VARCHAR(20);
"""

CREATE_WEEKLY_STATS_TABLE = """
CREATE TABLE IF NOT EXISTS weekly_stats (
    player_id VARCHAR(50) REFERENCES players(player_id) ON DELETE CASCADE,
    season INT,
    week INT NOT NULL,
    snap_share REAL DEFAULT 0.0,
    target_share REAL DEFAULT 0.0,
    route_rate REAL DEFAULT 0.0,
    high_value_touches INT DEFAULT 0,
    rush_share REAL,
    targets INT,
    carries INT,
    avg_separation REAL,
    avg_intended_air_yards REAL,
    avg_time_to_throw REAL,
    aggressiveness REAL,
    contested_ball_rate REAL,
    pressure_rate_allowed REAL,
    pass_block_win_rate REAL,
    depth_chart_rank INT,
    depth_chart_delta INT,
    team VARCHAR(10),
    opponent VARCHAR(10),
    is_home BOOLEAN,
    spread_line REAL,
    total_line REAL,
    team_moneyline INT,
    opponent_moneyline INT,
    team_implied_total REAL,
    projected_points REAL DEFAULT 0.0,
    actual_points REAL DEFAULT 0.0,
    vorp_score REAL DEFAULT 0.0,
    PRIMARY KEY (player_id, season, week)
);
"""

# Backfills the nflverse-sourced advanced metric columns (CORE-04) on weekly_stats tables
# created before this migration existed. Left nullable with no DEFAULT, unlike the original
# share columns, so "no data from this source this week" is a real NULL rather than a
# computed 0.0 that downstream VORP math would treat as a real zero.
ADD_WEEKLY_STATS_ADVANCED_COLUMNS = """
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS season INT;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS rush_share REAL;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS targets INT;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS carries INT;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS avg_separation REAL;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS avg_intended_air_yards REAL;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS avg_time_to_throw REAL;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS aggressiveness REAL;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS contested_ball_rate REAL;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS pressure_rate_allowed REAL;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS pass_block_win_rate REAL;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS depth_chart_rank INT;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS depth_chart_delta INT;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS team VARCHAR(10);
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS opponent VARCHAR(10);
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS is_home BOOLEAN;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS spread_line REAL;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS total_line REAL;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS team_moneyline INT;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS opponent_moneyline INT;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS team_implied_total REAL;
"""

# Backfills the VORP audit columns (CORE-05) on weekly_stats tables created before this
# migration existed. Nullable with no DEFAULT, same rationale as CORE-04's advanced
# columns: "not computed yet" must be a real NULL, not a 0.0 that VORP math would treat
# as a legitimate zero-value coefficient/baseline.
ADD_WEEKLY_STATS_VORP_COLUMNS = """
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS matchup_coefficient REAL;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS adjusted_projected_points REAL;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS vorp_base REAL;
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS scarcity_multiplier REAL;
"""

# Backfills the opponent-defense-strength multiplier (#93) on weekly_stats tables created
# before this migration existed. Distinct from matchup_coefficient above (which is the
# player's own team implied-total / individual-signal based multiplier) - this one is
# opponent-defense based, computed only when MATCHUP_WEIGHTING_ENABLED=true. Nullable with
# no DEFAULT, same "not computed yet must be a real NULL" rationale as the VORP audit
# columns: a 0.0/1.0 default would be indistinguishable from "computed and neutral".
ADD_WEEKLY_STATS_DEFENSE_MATCHUP_COLUMN = """
ALTER TABLE weekly_stats ADD COLUMN IF NOT EXISTS defense_matchup_multiplier REAL;
"""

# Re-keys weekly_stats on (player_id, season, week) so week 1 of one season no longer
# collides with week 1 of another. Idempotent: if the constraint has already been migrated,
# the DROP/ADD pair is skipped. Also skips (with a NOTICE, not an error) if existing rows
# have a NULL season - a NOT NULL PK column can't be added over them, and there's no
# reliable way to infer which season a pre-CORE-04 row belongs to, so that backfill has to
# be a deliberate, reviewed, one-off operation rather than something this migration guesses at.
MIGRATE_WEEKLY_STATS_PK_ADD_SEASON = """
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_name = 'weekly_stats' AND constraint_name = 'weekly_stats_pkey'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.key_column_usage
        WHERE table_name = 'weekly_stats' AND constraint_name = 'weekly_stats_pkey'
        AND column_name = 'season'
    ) THEN
        IF EXISTS (SELECT 1 FROM weekly_stats WHERE season IS NULL) THEN
            RAISE NOTICE 'Skipping weekly_stats PK migration to (player_id, season, week): % row(s) have a NULL season and need a manual, reviewed backfill first.',
                (SELECT count(*) FROM weekly_stats WHERE season IS NULL);
        ELSE
            ALTER TABLE weekly_stats DROP CONSTRAINT weekly_stats_pkey;
            ALTER TABLE weekly_stats ADD PRIMARY KEY (player_id, season, week);
        END IF;
    END IF;
END $$;
"""

CREATE_LEAGUE_ROSTERS_TABLE = """
CREATE TABLE IF NOT EXISTS league_rosters (
    team_id VARCHAR(50) NOT NULL,
    player_id VARCHAR(50) REFERENCES players(player_id) ON DELETE CASCADE,
    slot_position VARCHAR(20) NOT NULL,
    PRIMARY KEY (team_id, player_id)
);
"""

# Backfills is_starter (CORE-05) on league_rosters tables created before this column
# existed. True for any slot outside bench/IR (ESPN lineupSlotId 20/21).
ADD_LEAGUE_ROSTERS_IS_STARTER_COLUMN = """
ALTER TABLE league_rosters ADD COLUMN IF NOT EXISTS is_starter BOOLEAN;
"""

# League-level config synced from ESPN (CORE-05): team count, scoring format, and lineup
# slot counts, needed to compute replacement baselines and FLEX allocation. Single-league
# for now (no league_id FK elsewhere) - see CORE-05 planning notes for why multi-league
# support is deferred.
CREATE_LEAGUE_SETTINGS_TABLE = """
CREATE TABLE IF NOT EXISTS league_settings (
    league_id VARCHAR(50) PRIMARY KEY,
    season INT NOT NULL,
    name VARCHAR(100),
    team_count INT NOT NULL,
    scoring_type VARCHAR(30),
    lineup_slot_counts JSONB NOT NULL,
    scoring_settings JSONB,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# Team-id -> display-name mapping, synced from ESPN alongside rosters (sync_league.py).
# Kept separate from league_settings (one row per league) since this is one row per team.
# PK is (league_id, team_id) rather than just team_id in case a future multi-league setup
# reuses the same numeric ESPN team-id space across leagues.
CREATE_LEAGUE_TEAMS_TABLE = """
CREATE TABLE IF NOT EXISTS league_teams (
    league_id VARCHAR(50) NOT NULL,
    team_id VARCHAR(50) NOT NULL,
    team_name VARCHAR(200) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (league_id, team_id)
);
"""

# One row per browser-worker transaction (lineup update, waiver claim), written on
# completion (INFRA-06) so Grafana can chart worker execution latency/success rate.
CREATE_EXECUTION_LOG_TABLE = """
CREATE TABLE IF NOT EXISTS execution_log (
    id SERIAL PRIMARY KEY,
    transaction_type VARCHAR(30) NOT NULL,
    status VARCHAR(20) NOT NULL,
    duration_ms INTEGER,
    error_message TEXT,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# Backfills bot_name/trace_id on execution_log tables created before these columns existed.
# bot_name gives fantasy-browser-worker's non-queue entrypoints (draft_sync, check_espn_session)
# a real identity instead of a null/awkward transaction_type. trace_id lets a Grafana panel row
# built from this table jump straight into the matching Tempo trace.
ADD_EXECUTION_LOG_BOT_NAME_COLUMN = """
ALTER TABLE execution_log ADD COLUMN IF NOT EXISTS bot_name VARCHAR(50);
"""

ADD_EXECUTION_LOG_TRACE_ID_COLUMN = """
ALTER TABLE execution_log ADD COLUMN IF NOT EXISTS trace_id VARCHAR(32);
"""

# Backfills per-pick context columns for draft_pick_click rows (#43), so a click bot
# confirmation can be correlated back to a specific league/season/player instead of just
# recording that some draft_targets message succeeded or failed. season is required
# alongside league_id because a reactivated league keeps the same league_id across
# seasons - draft_picks/draft_slots are already keyed the same way. round_number (not
# pick_number) because the click bot's WS confirmation only carries the SELECTED frame's
# round, never an overall pick index - naming it pick_number would misleadingly imply
# the latter.
ADD_EXECUTION_LOG_DRAFT_CONTEXT_COLUMNS = """
ALTER TABLE execution_log ADD COLUMN IF NOT EXISTS league_id VARCHAR(50);
ALTER TABLE execution_log ADD COLUMN IF NOT EXISTS season INTEGER;
ALTER TABLE execution_log ADD COLUMN IF NOT EXISTS player_id VARCHAR(50);
ALTER TABLE execution_log ADD COLUMN IF NOT EXISTS player_name VARCHAR(100);
ALTER TABLE execution_log ADD COLUMN IF NOT EXISTS round_number INTEGER;
"""

# One row per live draft pick (CORE-08), sourced from fantasy-browser-worker's
# draft_picks:{leagueId} Redis Stream. Durable local mirror of draft state so the
# real-time listener can resume the undrafted pool after a restart mid-draft instead of
# replaying the entire Stream from scratch or, worse, guessing.
CREATE_DRAFT_PICKS_TABLE = """
CREATE TABLE IF NOT EXISTS draft_picks (
    league_id VARCHAR(50) NOT NULL,
    season INT NOT NULL,
    pick_number INT NOT NULL,
    round INT,
    team_id VARCHAR(50) NOT NULL,
    player_id VARCHAR(50) REFERENCES players(player_id) ON DELETE CASCADE,
    drafted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (league_id, season, pick_number)
);
"""

# Draft-day config for the browser-driven control plane (CORE-10): league/team/season the
# draft-sync Job (a wrapper around draft_matrix.py's listener) should start with. Single
# active config per league_id, upserted via ON CONFLICT - mirrors league_settings' single-
# row-per-league shape.
CREATE_DRAFT_CONFIG_TABLE = """
CREATE TABLE IF NOT EXISTS draft_config (
    league_id VARCHAR(50) PRIMARY KEY,
    team_id VARCHAR(50) NOT NULL,
    season INT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# Practice-mode tagging for the draft-day control plane (CORE-18) - lets a mock-draft-room
# config/Job/status be distinguished from a real "Draft Day 2026" run without a parallel
# table or route tree. Defaults every pre-existing row to 'live' so CORE-10 behavior is
# unchanged for configs stored before this migration ran.
ADD_DRAFT_CONFIG_MODE_COLUMN = """
ALTER TABLE draft_config ADD COLUMN IF NOT EXISTS mode VARCHAR(10) NOT NULL DEFAULT 'live';
"""

# Snake-draft team count for leagues that sync_league.py never touches. league_settings only
# ever holds the one real ESPN_LEAGUE_ID, so a practice/mock room has no team_count anywhere -
# which leaves every pick's round NULL, draft_slots empty, and on_the_clock_team permanently
# unknown. It lives here rather than in league_settings deliberately: load_league_config reads
# league_settings with `LIMIT 1` and no league filter, and load_lineup_slot_counts takes the
# most recently updated row for the season, so a synthetic practice row would silently hijack
# real VORP and lineup math. Nullable: it is derived from pick arrivals, so it is unknown until
# round 1 completes.
ADD_DRAFT_CONFIG_TEAM_COUNT_COLUMN = """
ALTER TABLE draft_config ADD COLUMN IF NOT EXISTS team_count INT;
"""

# Manual auto-publish kill switch (issue #108/#109). Defaults every pre-existing row to
# true so hold_and_publish's behavior is unchanged for leagues configured before this
# migration ran - it is an operator opt-out, not an opt-in.
ADD_DRAFT_CONFIG_AUTO_PUBLISH_ENABLED_COLUMN = """
ALTER TABLE draft_config ADD COLUMN IF NOT EXISTS auto_publish_enabled BOOLEAN NOT NULL DEFAULT true;
"""

# 'manual' (an operator used the web-ui/API toggle) or 'circuit_breaker' (draft_matrix.py
# tripped it automatically after repeated publish failures). NULL means auto-publish has
# never been disabled for this league. This distinction is what lets a future auto-recovery
# feature tell "a human turned this off on purpose, leave it alone" apart from "our own code
# turned this off as a safety net, and may turn it back on once the draft looks healthy again"
# - without it, both cases are indistinguishable from a single bool.
ADD_DRAFT_CONFIG_AUTO_PUBLISH_DISABLED_BY_COLUMN = """
ALTER TABLE draft_config ADD COLUMN IF NOT EXISTS auto_publish_disabled_by VARCHAR(32);
"""

# The raw ESPN player id every draft pick arrives with.
#
# players.player_id is Sleeper's id - it is the PK four other tables reference, and the whole
# projection/VORP/optimizer stack joins on it - so draft_picks has to key off it too, or the
# "already drafted" anti-join silently stops excluding drafted players. But the draft stream
# only ever carries ESPN's id, and the espn_id crosswalk that bridges them covers 845 of 1027
# players and no K or D/ST at all.
#
# Storing only the resolved id therefore loses every unresolvable pick, and a gap in
# pick_number breaks the gapless run team-count derivation needs and mis-sizes every later
# round. Storing the ESPN id *in* player_id is worse still: the two id spaces overlap
# numerically, so a pick lands on a real but entirely different player (ESPN 12483 is Matthew
# Stafford; internal 12483 is Jack Bech) and nothing downstream can tell it apart.
#
# So keep both. player_id stays nullable and FK-checked, holding the internal id when the
# crosswalk resolves it; espn_player_id always holds the stream's own id, so no pick is ever
# lost and unresolved ones can be backfilled as crosswalk coverage improves.
ADD_DRAFT_PICKS_ESPN_PLAYER_ID_COLUMN = """
ALTER TABLE draft_picks ADD COLUMN IF NOT EXISTS espn_player_id VARCHAR(20);
"""

# Whether a pick landed on a player we did NOT actually attempt to publish (issue #108, Task F).
#
# handle_pick_event compares an incoming pick event on our own team's slot against
# draft:{league_id}:last_attempted_target - the last player we actually tried to publish for
# that pick_number via publish_draft_target. A mismatch (or no recorded attempt at all, e.g.
# auto-publish was off or the recompute never produced a feasible candidate) means ESPN's
# clock ran out and the room autodrafted on our behalf instead of our recommendation landing.
#
# Only ever meaningful for our own team's picks - opponent picks have no "target we attempted"
# concept, so this stays NULL for them rather than false, which would misleadingly imply we
# evaluated and ruled out autodraft for a pick we never controlled.
ADD_DRAFT_PICKS_WAS_AUTODRAFTED_COLUMN = """
ALTER TABLE draft_picks ADD COLUMN IF NOT EXISTS was_autodrafted BOOLEAN DEFAULT NULL;
"""

# Per-team round-1 draft slot (CORE-10), inferred live from round-1 pick arrival order in
# draft_matrix.py's handle_pick_event - ESPN's API cannot supply draft-slot assignment
# pre-draft (league.draft stays empty until ESPN marks the draft fully complete), so this
# is the only reliable source. Rounds 2+ turn order is derived from this via standard snake
# math, not stored per-round.
CREATE_DRAFT_SLOTS_TABLE = """
CREATE TABLE IF NOT EXISTS draft_slots (
    league_id VARCHAR(50) NOT NULL,
    season INT NOT NULL,
    team_id VARCHAR(50) NOT NULL,
    slot_number INT NOT NULL,
    PRIMARY KEY (league_id, season, team_id)
);
"""

# One row per polled news/rumor item (CORE-13), deliberately schema-free of any
# sentiment/confidence interpretation - that's a downstream LLM concern (WORKER-13), not
# this ingestion layer's job. player_id uses ON DELETE SET NULL (not CASCADE, unlike
# draft_picks) because a signal row is ingestion history that must survive even if the
# matched player row is later removed. Dedup is enforced via UNIQUE(source,
# source_item_id) - the source's own native item ID (RSS guid, reddit fullname), not
# content_hash, since that's kept only as a secondary audit field.
CREATE_RAW_SIGNALS_TABLE = """
CREATE TABLE IF NOT EXISTS raw_signals (
    id SERIAL PRIMARY KEY,
    source VARCHAR(30) NOT NULL,
    source_item_id VARCHAR(255) NOT NULL,
    content_hash VARCHAR(64) NOT NULL,
    raw_text TEXT NOT NULL,
    url VARCHAR(500),
    published_at TIMESTAMP,
    player_id VARCHAR(50) REFERENCES players(player_id) ON DELETE SET NULL,
    match_confidence REAL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source, source_item_id)
);
"""

# One row per LLM provider-call attempt (WORKER-12), not just the winning attempt -- a
# fallback chain that tried and failed against groq before succeeding on gemini needs both
# rows for CORE-14's success/failure-rate and fallback-frequency panels to mean anything.
# error_type distinguishes a real/local rate limit from an ordinary request failure so those
# can be charted separately; fallback_triggered is just attempt_number > 1, stored directly
# rather than derived, so a panel can filter on it without a self-join back to attempt 1.
CREATE_LLM_CALL_METRICS_TABLE = """
CREATE TABLE IF NOT EXISTS llm_call_metrics (
    id SERIAL PRIMARY KEY,
    provider VARCHAR(30) NOT NULL,
    model VARCHAR(100) NOT NULL,
    call_site VARCHAR(50),
    attempt_number INT NOT NULL,
    success BOOLEAN NOT NULL,
    latency_ms INTEGER,
    error_type VARCHAR(50),
    fallback_triggered BOOLEAN NOT NULL DEFAULT FALSE,
    trace_id VARCHAR(32),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# One row per historical-season backtest run (CORE-15): our system's decisions vs. a
# true-optimal-in-hindsight lineup, a static no-waiver-moves roster, and a pure-ADP-drafted
# roster, so a future Grafana panel can chart strategy performance over time without this
# package needing any Grafana-specific code itself.
CREATE_BACKTEST_RESULTS_TABLE = """
CREATE TABLE IF NOT EXISTS backtest_results (
    id SERIAL PRIMARY KEY,
    season INT NOT NULL,
    run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    our_lineup_points REAL,
    optimal_lineup_points REAL,
    naive_baseline_points REAL,
    our_waiver_points REAL,
    no_moves_points REAL,
    our_draft_points REAL,
    pure_adp_draft_points REAL,
    summary_json JSONB
);
"""

# Cold-start defensive-strength-vs-position proxy (#93): last season's points-allowed by
# (defense_team, position), z-scored within position, used only when
# MATCHUP_WEIGHTING_ENABLED=true to nudge Week 1 lineup VORP. season is the upcoming season
# this row is used FOR (matches the join key vorp_matrix.py filters on); source_season is
# season - 1, the season the data is actually derived from, kept alongside the z-score for
# auditability. Composite natural key mirrors weekly_stats' own PK shape.
CREATE_DEFENSE_POSITION_STRENGTH_TABLE = """
CREATE TABLE IF NOT EXISTS defense_position_strength (
    defense_team VARCHAR(10) NOT NULL,
    position VARCHAR(10) NOT NULL,
    season INT NOT NULL,
    source_season INT NOT NULL,
    points_allowed_per_game REAL,
    strength_z REAL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (defense_team, position, season)
);
"""

# One row per automated bot decision (CORE-17/#35): the *why*, not the *whether* -
# execution_log already covers success/failure, this covers the justification (VORP
# delta, matchup multiplier, etc.) that drove the action. Written by src/api/routes.py's
# trigger_* handlers, since the reasoning only exists on the Python side that computes it
# before publishing to the click bots - the bots themselves never see it.
#
# target_player_id/drop_player_id are deliberately NOT FKs to players.player_id. The
# trigger endpoints receive two different id namespaces depending on action:
# trigger_waiver_claim/trigger_draft_pick get ESPN ids (validated by
# _reject_internal_player_id), but trigger_lineup_swap's SwapInstruction ids are raw
# internal players.player_id (Sleeper-namespaced), never translated. A blind FK here
# risks silently matching the wrong player via the same id-namespace collision
# _reject_internal_player_id's docstring warns about (ESPN 12483 is Matthew Stafford,
# internal 12483 is Jack Bech). execution_log.player_id (added by
# ADD_EXECUTION_LOG_DRAFT_CONTEXT_COLUMNS above) already sets this precedent.
#
# inputs is JSONB rather than fixed columns so rumor-signal (WORKER-13) or LLM-output
# (WORKER-12) justification can be added later with zero migration - no decision logic in
# this repo consumes either signal yet. season/week get their own typed columns (pulled
# out of inputs by the route handler) purely for cheap Grafana filtering.
CREATE_BOT_DECISIONS_TABLE = """
CREATE TABLE IF NOT EXISTS bot_decisions (
    id SERIAL PRIMARY KEY,
    tracking_id VARCHAR(36) NOT NULL,
    bot_name VARCHAR(50) NOT NULL,
    action VARCHAR(30) NOT NULL,
    team_id VARCHAR(50) NOT NULL,
    league_id VARCHAR(50),
    season INTEGER,
    week INTEGER,
    target_player_id VARCHAR(50),
    drop_player_id VARCHAR(50),
    inputs JSONB,
    decided_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# Append high-performance indices to ensure Grafana panel metrics execute in sub-milliseconds
CREATE_INDICES = """
CREATE INDEX IF NOT EXISTS idx_players_injury ON players(injury_status);
CREATE INDEX IF NOT EXISTS idx_weekly_stats_lookup ON weekly_stats(week);
CREATE INDEX IF NOT EXISTS idx_players_gsis_id ON players(gsis_id);
CREATE INDEX IF NOT EXISTS idx_players_espn_id ON players(espn_id);
CREATE INDEX IF NOT EXISTS idx_execution_log_executed_at ON execution_log(executed_at);
CREATE INDEX IF NOT EXISTS idx_execution_log_draft_pick_click ON execution_log(league_id, season, executed_at DESC) WHERE transaction_type = 'draft_pick_click';
CREATE INDEX IF NOT EXISTS idx_draft_picks_league_season ON draft_picks(league_id, season);
CREATE INDEX IF NOT EXISTS idx_draft_slots_league_season ON draft_slots(league_id, season);
CREATE INDEX IF NOT EXISTS idx_raw_signals_player_id ON raw_signals(player_id);
CREATE INDEX IF NOT EXISTS idx_raw_signals_ingested_at ON raw_signals(ingested_at);
CREATE INDEX IF NOT EXISTS idx_llm_call_metrics_created_at ON llm_call_metrics(created_at);
CREATE INDEX IF NOT EXISTS idx_llm_call_metrics_provider ON llm_call_metrics(provider, model);
CREATE INDEX IF NOT EXISTS idx_bot_decisions_bot_name_decided_at ON bot_decisions(bot_name, decided_at DESC);
CREATE INDEX IF NOT EXISTS idx_bot_decisions_tracking_id ON bot_decisions(tracking_id);
"""

