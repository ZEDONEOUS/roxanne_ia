from utilities.processing import Processing
from bs4 import BeautifulSoup
import pandas as pd
import requests
import PyPDF2
import json
import os
import re

class PDF_pr(Processing):
    def __init__(self):
        super().__init__()

    def find_html_text(self, df, total):
        text = ""
        content = []

        self.print_progress(total)

        try:
            r = requests.get(x, timeout=6)
            if 'html' in r.headers['Content-Type']:
                soup = BeautifulSoup(r.text, 'html.parser')
                content = soup.find_all("p")
        except (
                requests.exceptions.ConnectionError, 
                requests.exceptions.TooManyRedirects,
                requests.exceptions.InvalidURL,
                requests.exceptions.Timeout
            ):
            content = []

        if len(content) > 0:
            for row in content:
                if row.string:
                    text += row.string

        return text

    def extract_pdf_info(self):
        root = 'pdf'
        dirs = os.listdir(root)
        extracted_urls = []
        df = None

        df_pdf_temp = pd.read_csv("csv/news_filtered.csv", sep = "|")

        if len(df_pdf_temp) > 0:
            df = df_pdf_temp
        else:
            for dir in dirs:
                re_path = os.path.join(root, dir)
                files = os.listdir(re_path)
                for file in files:
                    if file.endswith('.pdf'):
                        pdf_file = open(os.path.join(re_path, file), 'rb')
                        pdf_reader = PyPDF2.PdfFileReader(pdf_file)
                        key = '/Annots'
                        uri = '/URI'
                        ank = '/A'

                        page = pdf_reader.getPage(0)
                        object_page = page.getObject()

                        if key in object_page:
                            ann = object_page[key]
                            for a in ann:
                                u = a.getObject()
                                if uri in u[ank]:
                                    extracted_urls.append({
                                        'url': u[ank][uri]
                                    })

            df = pd.DataFrame.from_dict(extracted_urls)
            df = df.drop_duplicates("url")
            df["url"] = df["url"].astype(str)
            df["ext"] = df["url"].str.extract(r'(?P<ext>\.\w+$)', expand = True)
            df["dom"] = df["url"].str.extract(r'(?P<dom>^\w+)', expand = True)
            df["ext"] = df["ext"].fillna("")
            df = df[df["ext"].isin(self.CONFIG["ADMITED_FILES"])]
            df = df[df["dom"].isin(self.CONFIG["ADMITED_PROTOCOL"])]

            print("\n---> OBTENER TEXTO EN HTML:\n")
            df["texto"] = df.apply(self.find_html_text, args = (len(df), ), axis = 1)
            self.PROGRESS = 0

        df = df[~pd.isna(df["texto"])]
        df = df[df["texto"] != ""]
        df["texto"] = df["texto"].str.replace(
            r'(?P<dot>[\.])(?P<word>\w)', 
            lambda x: x.group("dot") + " " + x.group("word")
        )

        df["texto"] = df["texto"].str.replace(
                r'(?P<pre>[a-z])(?P<upper>[A-Z])(?P<pos>[a-z])', 
            lambda x: x.group("pre") + ". " + x.group("upper") + x.group("pos")
        )

        df["texto"] = df["texto"].str.replace('\n', '. ')
        df["texto"] = df["texto"].str.replace('\r', '')
        df["texto"] = df["texto"].str.replace('"', "'")
        df["texto"] = df["texto"].str.replace("\xa0", "", regex = False)
        df["texto"] = df["texto"].str.replace("\s{2,}", " ", regex = True)

        df = self.find_hours_days(df)

        df["categorias"] = df["texto"].apply(self.find_category)

        print("\n---> OBTENER FRASES CLAVE:\n")
        df = df.apply(self.find_key_phrases, args = (len(df), ), axis = 1)
        self.PROGRESS = 0

        print("\n---> OBTENER ENTIDADES:\n")
        df = df.apply(self.find_entities, args = (len(df), ), axis = 1)
        self.PROGRESS = 0

        df.to_csv("csv/pdf_info_ext.csv", sep = "|", index = False)

