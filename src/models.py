# Declarative DDL statements establishing data metrics and time-series schemas

CREATE_PLAYERS_TABLE = """
CREATE TABLE IF NOT EXISTS players (
    player_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    position VARCHAR(10) NOT NULL,
    team VARCHAR(10) NOT NULL,
    status VARCHAR(20) NOT NULL,
    injury_status VARCHAR(20),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_WEEKLY_STATS_TABLE = """
CREATE TABLE IF NOT EXISTS weekly_stats (
    player_id VARCHAR(50) REFERENCES players(player_id) ON DELETE CASCADE,
    week INT NOT NULL,
    snap_share REAL DEFAULT 0.0,
    target_share REAL DEFAULT 0.0,
    route_rate REAL DEFAULT 0.0,
    high_value_touches INT DEFAULT 0,
    projected_points REAL DEFAULT 0.0,
    actual_points REAL DEFAULT 0.0,
    vorp_score REAL DEFAULT 0.0,
    PRIMARY KEY (player_id, week)
);
"""

CREATE_LEAGUE_ROSTERS_TABLE = """
CREATE TABLE IF NOT EXISTS league_rosters (
    team_id VARCHAR(50) NOT NULL,
    player_id VARCHAR(50) REFERENCES players(player_id) ON DELETE CASCADE,
    slot_position VARCHAR(20) NOT NULL,
    PRIMARY KEY (team_id, player_id)
);
"""

# Append high-performance indices to ensure Grafana panel metrics execute in sub-milliseconds
CREATE_INDICES = """
CREATE INDEX IF NOT EXISTS idx_players_injury ON players(injury_status);
CREATE INDEX IF NOT EXISTS idx_weekly_stats_lookup ON weekly_stats(week);
"""

