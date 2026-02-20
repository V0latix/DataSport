SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS countries (
    country_id TEXT PRIMARY KEY,
    iso2 TEXT,
    iso3 TEXT,
    name_en TEXT NOT NULL,
    name_fr TEXT
);

CREATE TABLE IF NOT EXISTS sports (
    sport_id TEXT PRIMARY KEY,
    sport_name TEXT NOT NULL,
    sport_slug TEXT NOT NULL UNIQUE,
    created_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS disciplines (
    discipline_id TEXT PRIMARY KEY,
    discipline_name TEXT NOT NULL,
    discipline_slug TEXT NOT NULL,
    sport_id TEXT NOT NULL,
    confidence REAL,
    mapping_source TEXT,
    created_at_utc TEXT NOT NULL,
    FOREIGN KEY (sport_id) REFERENCES sports(sport_id)
);

CREATE TABLE IF NOT EXISTS sources (
    source_id TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_type TEXT,
    license_notes TEXT,
    base_url TEXT
);

CREATE TABLE IF NOT EXISTS competitions (
    competition_id TEXT PRIMARY KEY,
    sport_id TEXT NOT NULL,
    name TEXT NOT NULL,
    season_year INTEGER,
    level TEXT,
    start_date TEXT,
    end_date TEXT,
    source_id TEXT,
    FOREIGN KEY (sport_id) REFERENCES sports(sport_id),
    FOREIGN KEY (source_id) REFERENCES sources(source_id)
);

CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    competition_id TEXT NOT NULL,
    discipline_id TEXT,
    gender TEXT,
    event_class TEXT,
    event_date TEXT,
    FOREIGN KEY (competition_id) REFERENCES competitions(competition_id),
    FOREIGN KEY (discipline_id) REFERENCES disciplines(discipline_id)
);

CREATE TABLE IF NOT EXISTS participants (
    participant_id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    display_name TEXT NOT NULL,
    country_id TEXT,
    FOREIGN KEY (country_id) REFERENCES countries(country_id)
);

CREATE TABLE IF NOT EXISTS results (
    event_id TEXT NOT NULL,
    participant_id TEXT NOT NULL,
    rank INTEGER,
    medal TEXT,
    score_raw TEXT,
    points_awarded REAL,
    PRIMARY KEY (event_id, participant_id),
    FOREIGN KEY (event_id) REFERENCES events(event_id),
    FOREIGN KEY (participant_id) REFERENCES participants(participant_id)
);

CREATE TABLE IF NOT EXISTS raw_imports (
    import_id TEXT PRIMARY KEY,
    source_id TEXT,
    fetched_at_utc TEXT,
    raw_path TEXT,
    status TEXT,
    error TEXT,
    FOREIGN KEY (source_id) REFERENCES sources(source_id)
);

CREATE TABLE IF NOT EXISTS sport_federations (
    sport_id TEXT NOT NULL,
    federation_qid TEXT NOT NULL,
    federation_name TEXT,
    PRIMARY KEY (sport_id, federation_qid),
    FOREIGN KEY (sport_id) REFERENCES sports(sport_id)
);

CREATE INDEX IF NOT EXISTS idx_disciplines_sport ON disciplines(sport_id);
CREATE INDEX IF NOT EXISTS idx_events_competition ON events(competition_id);
CREATE INDEX IF NOT EXISTS idx_results_rank ON results(rank);
CREATE INDEX IF NOT EXISTS idx_competitions_source ON competitions(source_id);
CREATE INDEX IF NOT EXISTS idx_raw_imports_source ON raw_imports(source_id);
"""


REFERENCE_SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS countries (
    country_id TEXT PRIMARY KEY,
    iso2 TEXT,
    iso3 TEXT,
    name_en TEXT NOT NULL,
    name_fr TEXT
);

CREATE TABLE IF NOT EXISTS sports (
    sport_id TEXT PRIMARY KEY,
    sport_name TEXT NOT NULL,
    sport_slug TEXT NOT NULL UNIQUE,
    created_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS disciplines (
    discipline_id TEXT PRIMARY KEY,
    discipline_name TEXT NOT NULL,
    discipline_slug TEXT NOT NULL,
    sport_id TEXT NOT NULL,
    confidence REAL,
    mapping_source TEXT,
    created_at_utc TEXT NOT NULL,
    FOREIGN KEY (sport_id) REFERENCES sports(sport_id)
);

CREATE TABLE IF NOT EXISTS sources (
    source_id TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_type TEXT,
    license_notes TEXT,
    base_url TEXT
);

CREATE TABLE IF NOT EXISTS sport_federations (
    sport_id TEXT NOT NULL,
    federation_qid TEXT NOT NULL,
    federation_name TEXT,
    PRIMARY KEY (sport_id, federation_qid),
    FOREIGN KEY (sport_id) REFERENCES sports(sport_id)
);
"""


COMPETITION_SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS countries (
    country_id TEXT PRIMARY KEY,
    iso2 TEXT,
    iso3 TEXT,
    name_en TEXT NOT NULL,
    name_fr TEXT
);

CREATE TABLE IF NOT EXISTS sports (
    sport_id TEXT PRIMARY KEY,
    sport_name TEXT NOT NULL,
    sport_slug TEXT NOT NULL UNIQUE,
    created_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS disciplines (
    discipline_id TEXT PRIMARY KEY,
    discipline_name TEXT NOT NULL,
    discipline_slug TEXT NOT NULL,
    sport_id TEXT NOT NULL,
    confidence REAL,
    mapping_source TEXT,
    created_at_utc TEXT NOT NULL,
    FOREIGN KEY (sport_id) REFERENCES sports(sport_id)
);

CREATE TABLE IF NOT EXISTS sources (
    source_id TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_type TEXT,
    license_notes TEXT,
    base_url TEXT
);

CREATE TABLE IF NOT EXISTS competitions (
    competition_id TEXT PRIMARY KEY,
    sport_id TEXT NOT NULL,
    name TEXT NOT NULL,
    season_year INTEGER,
    level TEXT,
    start_date TEXT,
    end_date TEXT,
    source_id TEXT,
    FOREIGN KEY (sport_id) REFERENCES sports(sport_id),
    FOREIGN KEY (source_id) REFERENCES sources(source_id)
);

CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    competition_id TEXT NOT NULL,
    discipline_id TEXT,
    gender TEXT,
    event_class TEXT,
    event_date TEXT,
    FOREIGN KEY (competition_id) REFERENCES competitions(competition_id),
    FOREIGN KEY (discipline_id) REFERENCES disciplines(discipline_id)
);

CREATE TABLE IF NOT EXISTS participants (
    participant_id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    display_name TEXT NOT NULL,
    country_id TEXT,
    FOREIGN KEY (country_id) REFERENCES countries(country_id)
);

CREATE TABLE IF NOT EXISTS results (
    event_id TEXT NOT NULL,
    participant_id TEXT NOT NULL,
    rank INTEGER,
    medal TEXT,
    score_raw TEXT,
    points_awarded REAL,
    PRIMARY KEY (event_id, participant_id),
    FOREIGN KEY (event_id) REFERENCES events(event_id),
    FOREIGN KEY (participant_id) REFERENCES participants(participant_id)
);
"""


LINEAGE_SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS sources (
    source_id TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_type TEXT,
    license_notes TEXT,
    base_url TEXT
);

CREATE TABLE IF NOT EXISTS raw_imports (
    import_id TEXT PRIMARY KEY,
    source_id TEXT,
    fetched_at_utc TEXT,
    raw_path TEXT,
    status TEXT,
    error TEXT,
    FOREIGN KEY (source_id) REFERENCES sources(source_id)
);
"""
