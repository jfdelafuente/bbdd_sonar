import sqlite3
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

insert_into_stats = """ INSERT INTO stats (aplicacion, repos, reliability_rating, reliability_label, sqale_rating, sqale_label, security_rating, security_label,
                        alert_status_ok, alert_status_label, dloc_rating, dloc_label, coverage_rating, coverage_label)
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """

datos_csv = "stats.csv"


class Database:
    def __init__(self, path: str):
        self.path = path

    def __enter__(self):
        self.connection = sqlite3.connect(self.path)
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            print(f"an error occurred: {exc_val}")
        self.connection.close()


def extract_from_csv(file_to_process) -> pd.DataFrame:
    dataframe = pd.read_csv(file_to_process, sep=";")
    return dataframe


def load_to_csv(targetfile, data_to_load):
    data_to_load.to_csv(targetfile, sep=";", encoding="utf-8", index=False)


def create_schema(conn):
    with open("./pybase/sql/stats.sql") as f:
        conn.executescript(f.read())


def getDistinctAplicaciones(db):
    apps = db.cursor.execute("SELECT DISTINCT aplicacion FROM metricas").fetchall()
    data = [app[0] for app in apps]
    return data


def getDistinctRepoByAplicacion(db, app):
    names = db.cursor.execute(
        "SELECT name FROM metricas where aplicacion=?", (app,)
    ).fetchall()
    data = [name[0] for name in names]
    return data


def getNumRepobyAplicacion(db, app):
    query = "SELECT count(name), aplicacion from metricas \
        WHERE aplicacion='{}' GROUP BY aplicacion".format(
        app
    )
    # print(query)
    record = db.cursor.execute(query).fetchone()
    if record is None:
        return 0
    return record[0]


def getLabelA(db, app, label):
    query = "SELECT count({}), aplicacion from metricas \
        WHERE aplicacion='{}' and {}='A' GROUP BY aplicacion".format(
        label, app, label
    )
    # print(query)
    record = db.cursor.execute(query).fetchone()
    if record is None:
        return 0
    return record[0]


def getStatus(db, app, label):
    query = "SELECT count(alert_status), aplicacion from metricas \
        WHERE aplicacion='{}' and alert_status = '{}' GROUP BY aplicacion".format(
        app, label
    )
    # print(query)
    record = db.cursor.execute(query).fetchone()
    if record is None:
        return 0
    return record[0]


def extract_stats_from_db(db):
    project_ids = []
    apps = getDistinctAplicaciones(db)
    for app in apps:
        project_ids.append(
            (
                app,
                getNumRepobyAplicacion(db, app),
                getLabelA(db, app, "reliability_label"),
                getLabelA(db, app, "sqale_label"),
                getLabelA(db, app, "security_label"),
                getStatus(db, app, "OK"),
                getLabelA(db, app, "dloc_label"),
                getLabelA(db, app, "coverage_label"),
            )
        )
    df_project = pd.DataFrame(
        project_ids,
        columns=[
            "aplicacion",
            "repos",
            "reliability_rating",
            "sqale_rating",
            "security_rating",
            "alert_status_ok",
            "dloc_rating",
            "coverage_rating",
        ],
    )
    return df_project


def create_stats(db, df_stats):
    for i, measure in df_stats.iterrows():
        project = (
            measure["aplicacion"],
            int(measure["repos"]),
            int(measure["reliability_rating"]),
            measure["reliability_label"],
            int(measure["sqale_rating"]),
            measure["sqale_label"],
            int(measure["security_rating"]),
            measure["security_label"],
            int(measure["alert_status_ok"]),
            measure["alert_status_label"],
            int(measure["dloc_rating"]),
            measure["dloc_label"],
            int(measure["coverage_rating"]),
            measure["coverage_label"],
        )
        db.cursor.execute(insert_into_stats, project)
        db.connection.commit()
    return df_stats


def transformar_data(df_stats):
    # A=0-0.05, B=0.06-0.1, C=0.11-0.20, D=0.21-0.5, E=0.51-1
    rating = ["E", "D", "C", "B", "A"]
    umbrales = [-1, 0.39, 0.49, 0.7, 0.9, 1]
    df_stats["dloc_label"] = pd.cut(
        x=(df_stats["dloc_rating"] / df_stats["repos"]), bins=umbrales, labels=rating
    )

    df_stats["coverage_label"] = pd.cut(
        x=(df_stats["coverage_rating"] / df_stats["repos"]),
        bins=umbrales,
        labels=rating,
    )

    df_stats["sqale_label"] = pd.cut(
        x=(df_stats["sqale_rating"] / df_stats["repos"]), bins=umbrales, labels=rating
    )

    df_stats["reliability_label"] = pd.cut(
        x=(df_stats["reliability_rating"] / df_stats["repos"]),
        bins=umbrales,
        labels=rating,
    )

    df_stats["alert_status_label"] = pd.cut(
        x=(df_stats["alert_status_ok"] / df_stats["repos"]),
        bins=umbrales,
        labels=rating,
    )

    df_stats["security_label"] = pd.cut(
        x=(df_stats["security_rating"] / df_stats["repos"]),
        bins=umbrales,
        labels=rating,
    )

    return df_stats


def main():
    database = r"" + os.environ["DATABASE"]
    with Database(database) as db:
        print("Creando schema ...")
        create_schema(db.connection)
        df_stats = extract_stats_from_db(db)

        print("Calculando datos ...")
        df_stats = transformar_data(df_stats)

        print("Cargando datos ...")
        df_stats = create_stats(db, df_stats)

        print(f"Creando fichero datos: {datos_csv}")
        load_to_csv(datos_csv, df_stats)


if __name__ == "__main__":
    main()
