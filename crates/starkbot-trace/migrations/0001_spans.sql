CREATE TABLE spans (id INTEGER PRIMARY KEY AUTOINCREMENT, trace_id TEXT NOT NULL, span_id TEXT NOT NULL UNIQUE, parent_span_id TEXT, name TEXT NOT NULL, kind INTEGER, start_ns INTEGER NOT NULL, end_ns INTEGER NOT NULL, duration_ms INTEGER NOT NULL, status_code INTEGER NOT NULL, status_message TEXT, service TEXT NOT NULL, surface TEXT, run TEXT, pid INTEGER, turn TEXT, operation TEXT, model TEXT, system TEXT, input_tokens INTEGER, output_tokens INTEGER, label TEXT NOT NULL, attributes TEXT NOT NULL CHECK (json_valid(attributes)), events TEXT NOT NULL CHECK (json_valid(events))) STRICT;
CREATE INDEX spans_start ON spans (start_ns);
CREATE INDEX spans_trace ON spans (trace_id);
CREATE INDEX spans_turn ON spans (turn);
CREATE INDEX spans_name ON spans (name);
CREATE INDEX spans_run ON spans (run);
CREATE VIEW runs AS SELECT run, MIN(service) AS service, MIN(surface) AS surface, MIN(pid) AS pid, MIN(start_ns) AS started_ns, MAX(end_ns) AS last_ns, COUNT(*) AS spans FROM spans WHERE run IS NOT NULL GROUP BY run;
