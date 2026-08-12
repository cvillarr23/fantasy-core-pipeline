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

# Append high-performance indices to ensure Grafana panel metrics execute in sub-milliseconds
CREATE_INDICES = """
CREATE INDEX IF NOT EXISTS idx_players_injury ON players(injury_status);
CREATE INDEX IF NOT EXISTS idx_weekly_stats_lookup ON weekly_stats(week);
CREATE INDEX IF NOT EXISTS idx_players_gsis_id ON players(gsis_id);
CREATE INDEX IF NOT EXISTS idx_players_espn_id ON players(espn_id);
CREATE INDEX IF NOT EXISTS idx_execution_log_executed_at ON execution_log(executed_at);
CREATE INDEX IF NOT EXISTS idx_draft_picks_league_season ON draft_picks(league_id, season);
"""

