CREATE TABLE runs (run TEXT PRIMARY KEY, source TEXT NOT NULL, pid INTEGER NOT NULL, started_ms INTEGER NOT NULL, last_ms INTEGER NOT NULL, records INTEGER NOT NULL, dropped INTEGER NOT NULL) STRICT;
CREATE TABLE records (id INTEGER PRIMARY KEY AUTOINCREMENT, seq INTEGER NOT NULL, ts_ms INTEGER NOT NULL, run TEXT NOT NULL, source TEXT NOT NULL, pid INTEGER NOT NULL, turn TEXT, kind TEXT NOT NULL, label TEXT NOT NULL, duration_ms INTEGER, ok INTEGER, provider TEXT, model TEXT, operation TEXT, surface TEXT, input_tokens INTEGER, output_tokens INTEGER, body TEXT NOT NULL CHECK (json_valid(body))) STRICT;
CREATE INDEX records_ts ON records (ts_ms);
CREATE INDEX records_turn ON records (turn);
CREATE INDEX records_run ON records (run);
CREATE INDEX records_kind ON records (kind);
