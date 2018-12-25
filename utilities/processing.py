from dateutil import parser
import pandas as pd
import urllib.request
import urllib.error
import urllib.parse
import numpy as np
import http.client
import random
import base64
import json
import ast
import re

class Processing():
    def __init__(self):
        self.ext_files = [
                { "type":"email", "name":"emails_info_ext.csv" },
                { "type":"pdf", "name":"pdf_info_ext.csv" },
                { "type":"tweet", "name":"tweets_info_ext.csv" }
        ]
        self.PROGRESS = 0
        self.HEADERS = {
            'Content-Type': 'application/json',
            'Ocp-Apim-Subscription-Key': '', # TOKEN DE SUBSCRIPCION
        }
        self.PLACES = []
        with open('utilities/config.json') as file:
            self.CONFIG = json.load(file)
            file.close()

    def print_progress(self, total):
        progress = format((self.PROGRESS * 100) / total, '.2f')
        print("---> Progreso: {}%".format(progress), end = "\r", flush = True)
        self.PROGRESS = self.PROGRESS + 1

    def clean_text(self, df):
        df = df[~pd.isna(df["texto"])]
        df = df[df["texto"] != ""]
        df = df.sort_values(by = ["texto"])
        df = df.drop_duplicates(subset = ["texto"], keep="last")

        df["texto"] = df["texto"].str.replace('\n', '. ')
        df["texto"] = df["texto"].str.replace('\r', '')
        df["texto"] = df["texto"].str.replace('"', "'")
        df["texto"] = df["texto"].str.replace("\xa0", "", regex = False)
        df["texto"] = df["texto"].str.replace("\s{2,}", " ", regex = True)
        #  df["texto"] = df["texto"].str.replace("(?:\d)([\.|\*|-]\s)(?:\d)", "/", regex = True)


        df["texto"] = df["texto"].str.replace(
            r'(?P<dot>[\.])(?P<word>[a-z|A-Z])', 
            lambda x: x.group("dot") + " " + x.group("word")
        )

        df["texto"] = df["texto"].str.replace(
                r'(?P<pre>[a-z])(?P<upper>[A-Z])(?P<pos>[a-z])', 
            lambda x: x.group("pre") + ". " + x.group("upper") + x.group("pos")
        )

        return df

    def find_category(self, df, total):
        self.print_progress(total)

        cats_arr = []
        for cat in self.CONFIG["CATEGORIES"]:
            if cat["texto"] in df["texto"].lower():
                cats_arr.append(cat["texto"])

        df["categorias"] = '^,'.join(cats_arr)

        return df

    def find_entities(self, df, total):
        self.print_progress(total)

        params = { "documents": [ { "language": "es", "id": 1, "text": df["texto"] } ] }

        json_obj = json.dumps(params)

        people = ""
        organizations = ""
        locations = ""
        quantities = ""
        datetimes = ""

        try:
            conn = http.client.HTTPSConnection('westeurope.api.cognitive.microsoft.com')
            conn.request("POST", "/text/analytics/v2.1-preview/entities", json_obj, self.HEADERS)
            response = conn.getresponse()
            data = response.read()
            dict_data = json.loads( data )
            conn.close()
        except Exception as e:
            print("[Errno {0}] {1}".format(e.errno, e.strerror))

        if len(dict_data["Documents"]) > 0:
            for entitie in dict_data["Documents"][0]["Entities"]:
                if "Type" in entitie:
                    if entitie["Type"] == "Person":
                        people +=  "^," + entitie["Name"]
                    if entitie["Type"] == "Organization":
                        organizations += "^," + entitie["Name"]
                    if entitie["Type"] == "Location":
                        locations += "^," + entitie["Name"]
                    if entitie["Type"] == "Quantity":
                        quantities += "^," + entitie["Name"]
                    if entitie["Type"] == "DateTime":
                        datetimes += "^," + entitie["Name"]

        df["personas"] = people
        df["organizaciones"] = organizations
        df["lugares"] = locations 
        df["cantidades"] = quantities
        df["fechastiempos"] = datetimes

        return df

    def find_key_phrases(self, df, total):
        self.print_progress(total)

        params = { "documents": [ { "language": "es", "id": 1, "text": df["texto"] } ] }
        json_obj = json.dumps(params)

        keyphrases = ""

        try:
            conn = http.client.HTTPSConnection('westeurope.api.cognitive.microsoft.com')
            conn.request("POST", "/text/analytics/v2.0/keyPhrases", json_obj, self.HEADERS)
            response = conn.getresponse()
            data = response.read()
            dict_data = json.loads(data)
            conn.close()
        except Exception as e:
            print("[Errno {0}] {1}".format(e.errno, e.strerror))

        if len(dict_data["documents"]) > 0:
            if 'keyPhrases' in dict_data["documents"][0]:
                keyphrases = "^,".join(dict_data["documents"][0]["keyPhrases"])

        df["frasesclave"] = keyphrases

        return df

    def find_hours_days(self, df, total):
        self.print_progress(total)

        horas = re.search(
            r'(?P<horas>\d+(?=.{0}[ ]{0,5}?[hH][oO][rR][aA][sS]?))', 
            df["texto"]
        )

        df["horas"] = int(horas.group(1)) if horas != None else 0

        dias = re.search(
            r'(?P<dias>\d+(?=.{0}[ ]{0,5}?[dD][iíIÍ][aA][sS]?))', 
            df["texto"]
        )

        df["dias"] = int(dias.group(1)) if dias != None else 0

        return df

    def summary_ext_info(self, archivo):
        df = pd.read_csv(archivo, sep = "|")

        print("\n---> RESUMEN DATAFRAME: \n\n")
        print(df)

        print("\n---> TOTALIDAD DE DATOS: \n\n")
        print(len(df))

        print("\n---> COLUMNAS EN ARCHIVO: \n\n")
        print(str(df.dtypes))

        print("\n---> NULOS POR COLUMNA: \n\n")
        for column in df.columns:
            print(column + ": " + str(df[column].isna().sum()))

    def process_categories(self, df, total):
        self.print_progress(total)

        cats = "N/A"
        riesgo = 0
        if df["categorias"] != "":
            cats = df["categorias"].replace("^,", " - ").upper()
            for risk_cat in self.CONFIG["CATEGORIES"]:
                if risk_cat["texto"].upper() in cats:
                    if int(risk_cat["riesgo"]) > riesgo:
                        riesgo = int(risk_cat["riesgo"])
                    else:
                        pass
                else:
                    pass

        df["categorias"] = cats
        df["riesgo"] = riesgo

        return df

    def process_hours_days(self, df, total):
        self.print_progress(total)

        riesgo = 0
        if df["riesgo"] <= 1:
            if df["horas"] < 10:
                riesgo = 1
            if df["horas"] >= 10:
                riesgo = 2
            if df["dias"] > 0:
                riesgo = 3
        else:
            riesgo = df["riesgo"]

        df["riesgo"] = riesgo

        return df

    def process_datetimes(self, df, total):
        self.print_progress(total)

        datetimes = "N/A"
        temp_arr = []
        if df["tipo"] == "tweet":
            try:
                datetimes = str(parser.parse(df["fecha_creacion"])).split(" ")[0]
            except Exception as e:
                datetimes = "N/A"
        else:
            if len(df["fechastiempos"]) > 0:
                datetimes_arr = df["fechastiempos"].split("^,")
                for datetime in datetimes_arr:
                    if len(datetime) == 10:
                        texto = re.sub('[\.|\*|-]', '/', datetime)
                        temp_arr.append(texto)
                if len(temp_arr) > 0:
                    try:
                        datetimes = str(parser.parse(sorted(temp_arr)[0])).split(" ")[0]
                    except Exception as e:
                        datetimes = "N/A"

        df["fechas"] = datetimes

        return df

    def reduce_entities(self, df, param, min, total):
        self.print_progress(total)

        entities = "N/A"
        temp_arr = []
        if len(df[param]) > 0:
            entities_arr = df[param].split("^,")
            for entitie in entities_arr:
                if len(entitie) > min:
                    temp_arr.append(entitie.upper())
            entities = " - ".join(sorted(temp_arr))

        df[param] = entities

        return df

    def export_compiled_files(self):
        df = pd.DataFrame()
        for file in self.ext_files:
            df_file = pd.read_csv("csv/" + file["name"], sep = "|")
            df_file["tipo"] = file["type"]
            if len(df) > 0:
                df = df.append(df_file, ignore_index = True, sort = False)
            else:
                df= df_file   

        df = df.fillna("")
        df["horas"] = df["horas"].replace("", 0)
        df["horas"] = df["horas"].astype(int)
        df["dias"] = df["dias"].replace("", 0)
        df["dias"] = df["dias"].astype(int)
        df["texto"] = df["texto"].str.replace("\xa0", "", regex = False)
        df["texto"] = df["texto"].str.replace("\s{2,}", " ", regex = True)

        #  df = df[1000:2000]

        print('\n\n---> PROCESA CATEGORIAS')
        df = df.apply(self.process_categories, args = (len(df), ), axis = 1)
        self.PROGRESS = 0
        print('\n\n---> PROCESA HORAS Y DIAS')
        df = df.apply(self.process_hours_days, args = (len(df), ), axis = 1)
        self.PROGRESS = 0
        print('\n\n---> PROCESA FECHAS')
        df = df.apply(self.process_datetimes, args = (len(df), ), axis = 1)
        self.PROGRESS = 0
        print('\n\n---> PROCESA LUGARES')
        df = df.apply(self.reduce_entities, args = ("lugares", 3, len(df)), axis = 1)
        self.PROGRESS = 0
        print('\n\n---> PROCESA FRASES CLAVE')
        df = df.apply(self.reduce_entities, args = ("frasesclave", 15, len(df)), axis = 1)
        self.PROGRESS = 0
        print('\n\n---> PROCESA ORGANIZACIONES')
        df = df.apply(self.reduce_entities, args = ("organizaciones", 7, len(df)), axis = 1)
        self.PROGRESS = 0

        df = df.drop(columns = [
            "fechastiempos", 
            "fecha_creacion", 
            "cantidades", 
            "ext", 
            "dom", 
            "ciudades",
            "lugar"
        ])

        print("\n")

        print(len(df))

        #  df = df[df["fechas"] != "N/A"]

        print(len(df))

        print(df[(df["riesgo"] > 2) & (df["lugares"] != "N/A")][["riesgo", "lugares", "frasesclave", "categorias", "dias"]][:8])
        
        print("\n")

        #  print(df.groupby(["tipo","lugares"]).agg({
            #  "fechas":["count"]
        #  }))

        df.to_csv("csv/compiled_files.csv", index = False, sep = "|")
