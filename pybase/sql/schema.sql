DROP TABLE IF EXISTS metricas;

CREATE TABLE metricas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    aplicacion TEXT,
    fecha TEXT,
    bugs INTEGER,
    reliability_rating INTEGER,
    reliability_label TEXT,
    vulnerabilities INTEGER,
    security_rating INTEGER,
    security_label TEXT,
    code_smells INTEGER,
    sqale_rating INTEGER,
    sqale_label TEXT,
    alert_status TEXT,
    app_sonar TEXT,
    complexity INTEGER,
    coverage INTEGER,
    ncloc INTEGER,
    duplicated_line_density INTEGER,
    sqale_index INTEGER,
    size TEXT,
    dloc_label TEXT,
    coverage_label TEXT
);

DROP TABLE IF EXISTS historico;

CREATE TABLE historico (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    aplicacion TEXT,
    fecha TEXT,
    bugs INTEGER,
    reliability_rating INTEGER,
    reliability_label TEXT,
    vulnerabilities INTEGER,
    security_rating INTEGER,
    security_label TEXT,
    code_smells INTEGER,
    sqale_rating INTEGER,
    sqale_label TEXT,
    alert_status TEXT,
    app_sonar TEXT,
    complexity INTEGER,
    coverage INTEGER,
    ncloc INTEGER,
    duplicated_line_density INTEGER,
    sqale_index INTEGER,
    size TEXT,
    dloc_label TEXT,
    coverage_label TEXT
);

DROP TABLE IF EXISTS proveedor;

CREATE TABLE proveedor (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    aplicacion TEXT,
    proveedor TEXT
);