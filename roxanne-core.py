from utilities.processing import Processing
from utilities.pdf_pr import PDF_pr
from utilities.tweets_pr import TWEETS_pr
from utilities.email_pr import EMAIL_pr
import pandas as pd
import textwrap
import time
import re

class Main:
    def __init__(self):
        self.pdf_pr = PDF_pr()
        self.tweets_pr = TWEETS_pr()
        self.email_pr = EMAIL_pr()
        self.processing = Processing()

    def roxanne_credits(self):
        print(textwrap.dedent("""
        ______                                         
        | ___ \                                        
        | |_/ /  ___  __  __  __ _  _ __   _ __    ___ 
        |    /  / _ \ \ \/ / / _` || '_ \ | '_ \  / _ \\
        | |\ \ | (_) | >  < | (_| || | | || | | ||  __/
        \_| \_| \___/ /_/\_\ \__,_||_| |_||_| |_| \___|
         ______________________________________________
        |______________________________________________|
        """))

    def menu(self):
        print(textwrap.dedent("""
        Ingrese una de las siguientes opciones:

        (1) Extraer informacion de PDF's
        (2) Resumen analisis informacion de PDF's
        (3) Extraer informacion registro historico de Tweets
        (4) Extraer informacion registro 10 dias de Tweets
        (5) Resumen analisis informacion registro Tweets
        (6) Extraer informacion registro E-Mails
        (7) Resumen analisis informacion registro E-Mails
        (8) Exportar archivo compilado de análisis
        (q) Terminar Programa """))

    def switch_menu(self, param):
        switcher = {
            "1": self.pdf_info_ext,
            "2": self.pdf_info_summ,
            "3": self.tweets_info_ext,
            "4": self.tweets_last_info_ext,
            "5": self.tweets_info_summ,
            "6": self.email_info_ext,
            "7": self.email_info_summ,
            "8": self.export_compiled_files
        }
        option = switcher.get(param, False)

        if option == False:
            print("---> Opcion no encontrada!")
        else:
            option()

    def pdf_info_ext(self):
        print(textwrap.dedent("""
        A continuación se analizaran los archivos de extensión [.pdf]
        encontrados dentro la carpeta pdf separados por carpetas internamente

        Posterior a esto se exportara un archivo de nombre pdf_info_ext.csv
        encontrado en la direccion csv/pdf_info_ext.csv """))

        entry = input("\n¿ Desea proceder con la solicitud ? [s/n]: ")

        if entry == "s":
            start_time = time.time()
            self.pdf_pr.extract_pdf_info()
            print("")
            print("\n---> Noticias en archivos pdf analizados satisfactoriamente!")
            print("\n---> Tiempo que tardo la ejecucion: %s Segundos", (time.time() - start_time))

    def pdf_info_summ(self):
        print(textwrap.dedent("""
        A continuación se presentara un resumen de la iformacion extraida del
        analisis mediante el procesamiento de lenguage natural de los pdf's con 
        noticias.

        Se hara la lectura del archivo compilado del analisis 
        encontrado en la direccion csv/pdf_info_ext.csv """))

        entry = input("\n¿ Desea proceder con la solicitud ? [s/n]: ")

        if entry == "s":
            print("\n---> Resultado del análisis de pdf's:")
            self.pdf_pr.summary_ext_info('csv/pdf_info_ext.csv')

    def tweets_last_info_ext(self):
        print(textwrap.dedent("""
        A continuacion se analizaran los ultimos 10 dias de tweets registrados 
        relacionados con las categorias suscritas en el archivo de configuracion

        Posterior a esto se exportara un archivo de nombre tweets_last_info_ext.csv
        encontrado en la direccion csv/tweets_last_info_ext.csv """))

        entry = input("\n¿ Desea proceder con la solicitud ? [s/n]: ")

        if entry == "s":
            start_time = time.time()
            self.tweets_pr.extract_last_tweets_info()
            print("\n---> Tweets en archivos analizados satisfactoriamente!")
            print("\n---> Tiempo que tardo la ejecucion: %s Segundos", (time.time() - start_time))

    def tweets_info_ext(self):
        print(textwrap.dedent("""
        A continuación se analizaran los archivos de extensión [.csv]
        encontrados dentro la carpeta csv/tweets_raw/ 

        Posterior a esto se exportara un archivo de nombre tweets_info_ext.csv
        encontrado en la direccion csv/tweets_info_ext.csv """))

        entry = input("\n¿ Desea proceder con la solicitud ? [s/n]: ")

        if entry == "s":
            start_time = time.time()
            self.tweets_pr.extract_tweets_info()
            print("\n---> Tweets en archivos analizados satisfactoriamente!")
            print("\n---> Tiempo que tardo la ejecucion: %s Segundos", (time.time() - start_time))

    def tweets_info_summ(self):
        print(textwrap.dedent("""
        A continuación se presentara un resumen de la iformacion extraida del
        analisis mediante el procesamiento de lenguage natural de los trinos
        extraidos de la red social Twitter.

        Se hara la lectura del archivo compilado del analisis 
        encontrado en la direccion csv/tweets_info_ext.csv """))

        entry = input("\n¿ Desea proceder con la solicitud ? [s/n]: ")

        if entry == "s":
            print("\n---> Resultado del análisis de trinos:")
            self.pdf_pr.summary_ext_info('csv/tweets_info_ext.csv')

    def email_info_ext(self):
        print(textwrap.dedent("""
        A continuación se analizaran los emails enviados por comunicaciones
        encontrados en el correo electronico al cual se acordo el envio periodico

        Posterior a esto se exportara un archivo de nombre emails_info_ext.csv
        encontrado en la direccion csv/emails_info_ext.csv """))

        entry = input("\n¿ Desea proceder con la solicitud ? [s/n]: ")

        if entry == "s":
            start_time = time.time()
            self.email_pr.extract_email_info()
            print("\n---> Emails en archivos analizados satisfactoriamente!")
            print("\n---> Tiempo que tardo la ejecucion: %s Segundos", (time.time() - start_time))

    def email_info_summ(self):
        print(textwrap.dedent("""
        A continuación se presentara un resumen de la iformacion extraida del
        analisis mediante el procesamiento de lenguage natural de los E-Mails
        extraidos de los ultimos 10 dias segun información proporcionada.

        Se hara la lectura del archivo compilado del analisis 
        encontrado en la direccion csv/emails_info_ext.csv """))

        entry = input("\n¿ Desea proceder con la solicitud ? [s/n]: ")

        if entry == "s":
            print("\n---> Resultado del análisis de emails:")
            self.email_pr.summary_ext_info('csv/emails_info_ext.csv')

    def export_compiled_files(self):
        print(textwrap.dedent("""
        A continuación se compilara la informacion obtenida del analisis de 
        Noticias, E-Mails y Trinos capturados.

        se exportara un archivo compilado con los resultados del análisis
        en la ruta csv/compiled_files.csv """))

        entry = input("\n¿ Desea proceder con la solicitud ? [s/n]: ")

        if entry == "s":
            self.processing.export_compiled_files()
            print("\n---> Archivo compilado exportado!")

if __name__ == '__main__':
    main = Main()
    main_loop = True

    main.roxanne_credits()
    
    while main_loop:
        main.menu()
        entry = input("\nOpcion seleccionada: ")

        if entry == "q":
            print("---> Programa Terminado!")

            main_loop = False
        elif re.search(r"\d", entry):
            main.switch_menu(entry)
        else:
            print("---> Opción no permitida!")
        time.sleep(1)
        print("_______________________________________________")
