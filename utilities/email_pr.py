from utilities.processing import Processing
from bs4 import BeautifulSoup
import pandas as pd
import imaplib
import email
import re

class EMAIL_pr(Processing):
    def __init__(self):
        super().__init__()

    def clean_tags(self, df):
        texto = re.sub(r'\<(?:.*?)\>', ' ', str(df["tags"]))

        df["texto"] = texto

        return df

    def read_mails(self):
        tags_ext = []
        mail = imaplib.IMAP4_SSL(self.CONFIG["SMTP_SERVER"])
        mail.login(self.CONFIG["EMAIL"], self.CONFIG["PASS"])
        mail.select('PQR')

        _, mails = mail.search(None, "ALL")
        mails_id = mails[0].split()
        total_mails = len(mails_id)
        fraction = total_mails - (total_mails // 6)

        print("\n\n---> OBTENER EMAILS:\n")

        for i in range(fraction, total_mails):
            self.print_progress(fraction)
            text = ""
            content = []
            typ, data = mail.fetch(mails_id[i],'(RFC822)')
            raw_email = data[0][1].decode('utf-8')
            msg = email.message_from_string(raw_email)
            decoded_msg = msg.get_payload(decode = True)
            if decoded_msg:
                soup = BeautifulSoup(decoded_msg, 'html.parser')
                content = soup.find_all("table")
                if len(content) > 0:
                    for row in content:
                        text += str(row)
                    tags_ext.append(text)

        self.PROGRESS = 0
        return tags_ext

    def extract_email_info(self):
        tags_ext = self.read_mails()

        df = pd.DataFrame(data = tags_ext, columns = ["tags"])
        df = df.apply(self.clean_tags, axis = 1)
        df = df.drop(columns = ["tags"])

        df = self.clean_text(df)

        print("\n\n---> OBTENER DIAS Y HORAS:\n")
        df = df.apply(self.find_hours_days, args = (len(df), ), axis = 1)
        self.PROGRESS = 0

        print("\n\n---> OBTENER CATEGORIAS:\n")
        df = df.apply(self.find_category, args = (len(df), ), axis = 1)
        self.PROGRESS = 0

        print("\n\n---> OBTENER FRASES CLAVE:\n")
        df = df.apply(self.find_key_phrases, args = (len(df), ), axis = 1)
        self.PROGRESS = 0

        print("\n\n---> OBTENER ENTIDADES:\n")
        df = df.apply(self.find_entities, args = (len(df), ), axis = 1)
        self.PROGRESS = 0

        df.to_csv("csv/emails_info_ext.csv", sep = "|", index = False)


