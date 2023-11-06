DROP TABLE IF EXISTS stats;

CREATE TABLE stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    aplicacion TEXT,
    repos INTEGER,
    reliability_label TEXT,
    reliability_rating INTEGER,
    sqale_label TEXT,
    sqale_rating INTEGER,
    security_label TEXT,
    security_rating INTEGER,
    alert_status_label TEXT,
    alert_status_ok INTEGER,
    dloc_label TEXT,
    dloc_rating INTEGER,
    coverage_label TEXT,
    coverage_rating INTEGER
);

