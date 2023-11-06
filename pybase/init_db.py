import sqlite3
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

insert_into_metricas = """ INSERT INTO metricas   (name, aplicacion, fecha, bugs, reliability_rating, reliability_label, vulnerabilities, security_rating, security_label, code_smells, sqale_rating, sqale_label, alert_status, app_sonar, complexity, coverage,ncloc, duplicated_line_density, sqale_index, size, dloc_label, coverage_label)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?) """

insert_into_historico = """ INSERT INTO historico (name, aplicacion, fecha, bugs, reliability_rating, reliability_label, vulnerabilities, security_rating, security_label, code_smells, sqale_rating, sqale_label, alert_status, app_sonar, complexity, coverage,ncloc, duplicated_line_density, sqale_index, size, dloc_label, coverage_label)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?) """

insert_into_proveedor = """ INSERT INTO proveedor (aplicacion, proveedor)
                VALUES(?, ?) """

datos_csv = "metricas.csv"
historico_csv = "historico.csv"
proveedor_csv = "proveedores.csv"


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


def transformar_data():
    print("Modificarmos datos entrada ...")
    datos_in = r"" + os.environ["DATOS_CSV"]
    historico_in = r"" + os.environ["HISTORICO_CSV"]
    tallas = ["XS", "S", "M", "L", "XL"]
    rating = ["A", "B", "C", "D", "E"]
    umbrales_sizes = [-1, 1000, 10000, 100000, 500000, 10000000]
    umbrales_dloc = [-1, 3, 5, 10, 20, 100]
    umbrales_coverage = [-1, 30, 50, 60, 70, 100]

    data = extract_from_csv(datos_in)
    historico = extract_from_csv(historico_in)

    # Empiezan las transformaciones
    data.fillna(0, inplace=True)
    historico.fillna(0, inplace=True)

    # create a new column, date_parsed, with the parsed dates
    data["date_parsed"] = pd.to_datetime(data["date"], format="%Y-%m-%d %H:%M:%S")
    # get the day of the month from the date_parsed column
    data["day_of_month"] = data["date_parsed"].dt.day
    data["month_of_year"] = data["date_parsed"].dt.month
    data["year"] = data["date_parsed"].dt.year

    data["reliability_label"] = data["reliability_rating"]
    data["sqale_label"] = data["sqale_rating"]
    data["security_label"] = data["security_rating"]
    data["reliability_label"].replace([1, 2, 3, 4, 5], rating, inplace=True)
    data["sqale_label"].replace([1, 2, 3, 4, 5], rating, inplace=True)
    data["security_label"].replace([1, 2, 3, 4, 5], rating, inplace=True)
    data["size"] = pd.cut(x=data["ncloc"], bins=umbrales_sizes, labels=tallas)
    data["dloc_label"] = pd.cut(
        x=data["duplicated_lines_density"], bins=umbrales_dloc, labels=rating
    )
    data["coverage_label"] = pd.cut(
        x=100 - data["coverage"], bins=umbrales_coverage, labels=rating
    )

    historico["reliability_label"] = historico["reliability_rating"]
    historico["sqale_label"] = historico["sqale_rating"]
    historico["security_label"] = historico["security_rating"]
    historico["reliability_label"].replace([1, 2, 3, 4, 5], rating, inplace=True)
    historico["sqale_label"].replace([1, 2, 3, 4, 5], rating, inplace=True)
    historico["security_label"].replace([1, 2, 3, 4, 5], rating, inplace=True)
    historico["size"] = pd.cut(x=historico["ncloc"], bins=umbrales_sizes, labels=tallas)
    historico["dloc_label"] = pd.cut(
        x=historico["duplicated_lines_density"], bins=umbrales_dloc, labels=rating
    )
    historico["coverage_label"] = pd.cut(
        x=100 - historico["coverage"], bins=umbrales_coverage, labels=rating
    )

    load_to_csv(datos_csv, data)
    load_to_csv(historico_csv, historico)
    # data.to_csv(datos_csv, index=False, sep=";")
    # historico.to_csv(historico_csv, index=False, sep=";")


def create_schema(conn):
    with open("./pybase/sql/schema.sql") as f:
        conn.executescript(f.read())


def create_metricas(db):
    print("Extract datos metricas csv ...")
    df_measures = extract_from_csv(datos_csv)
    for i, measure in df_measures.iterrows():
        project = (
            measure["proyecto"] + "-application-" + measure["lenguaje"],
            measure["aplicacion"],
            measure["date"],
            int(measure["bugs"]),
            int(measure["reliability_rating"]),
            measure["reliability_label"],
            int(measure["vulnerabilities"]),
            int(measure["security_rating"]),
            measure["security_label"],
            int(measure["code_smells"]),
            int(measure["sqale_rating"]),
            measure["sqale_label"],
            measure["alert_status"],
            measure["app_sonar"],
            int(measure["complexity"]),
            int(measure["coverage"]),
            int(measure["ncloc"]),
            int(measure["duplicated_lines_density"]),
            int(measure["sqale_index"]),
            measure["size"],
            measure["dloc_label"],
            measure["coverage_label"],
        )

        db.cursor.execute(insert_into_metricas, project)
        db.connection.commit()
    print(db.cursor.lastrowid)


def create_historico(db):
    print("Extract datos historico csv ...")
    df_measures = extract_from_csv(historico_csv)
    for i, measure in df_measures.iterrows():
        project = (
            measure["proyecto"] + "-application-" + measure["lenguaje"],
            measure["aplicacion"],
            measure["date"],
            int(measure["bugs"]),
            int(measure["reliability_rating"]),
            measure["reliability_label"],
            int(measure["vulnerabilities"]),
            int(measure["security_rating"]),
            measure["security_label"],
            int(measure["code_smells"]),
            int(measure["sqale_rating"]),
            measure["sqale_label"],
            measure["alert_status"],
            measure["app_sonar"],
            int(measure["complexity"]),
            int(measure["coverage"]),
            int(measure["ncloc"]),
            int(measure["duplicated_lines_density"]),
            int(measure["sqale_index"]),
            measure["size"],
            measure["dloc_label"],
            measure["coverage_label"],
        )

        db.cursor.execute(insert_into_historico, project)
        db.connection.commit()
    print(db.cursor.lastrowid)


def create_proveedor(db):
    print("Extract datos proveedor csv ...")
    df_measures = extract_from_csv(proveedor_csv)
    for i, measure in df_measures.iterrows():
        project = (measure["aplicacion"], measure["proveedor"])

        db.cursor.execute(insert_into_proveedor, project)
        db.connection.commit()
    print(db.cursor.lastrowid)


def main():
    transformar_data()
    database = r"" + os.environ["DATABASE"]
    with Database(database) as db:
        print(
            "Datos metricas : %s \nDatos Historicos : %s \nDatos Proveedor : %s"
            % (datos_csv, historico_csv, proveedor_csv)
        )
        print("Creando schema ...")
        create_schema(db.connection)
        print("Cargando datos ...")
        create_metricas(db)
        create_historico(db)
        create_proveedor(db)


if __name__ == "__main__":
    main()
