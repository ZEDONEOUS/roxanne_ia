from utilities.processing import Processing
import pandas as pd
import datetime
import json
import os

class TWEETS_pr(Processing):
    def __init__(self):
       super().__init__()

    def get_last_tweets(self):
        pass 

    def extract_tweets_info(self):
        files = os.listdir("csv/tweets_raw/")
        df = pd.DataFrame()
        for file in files:
            df_file = pd.read_csv("csv/tweets_raw/" + file, sep = "|")
            if len(df) > 0:
                df = df.append(df_file, ignore_index=True, sort = False)
            else:
                df = df_file   

        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        df = df[df["texto"].str.find("�") == -1]
        df = df.sort_values(by = "tipo_lugar")
        df = df.sort_values(by = "conteo_retweets")
        df = df.drop_duplicates(["texto"], keep="last")
        df = df.drop(columns=["autor", "empresas", "palabras_clave", "tipo_lugar"])

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

        df.to_csv("csv/tweets_info_ext.csv", sep = "|", index = False)

    def extract_last_tweets_info(self):
        pass

    def create_tranning_data():
        df_tweets = fetch_clean_data()
        df_tweets["lugar_start"] = df_tweets[~pd.isna(df_tweets["lugar"])].apply(
            lambda x: int(x["texto"].find(x["lugar"])), 
            axis = 1
        )
        df_tweets["lugar_end"] = df_tweets["lugar_start"] + df_tweets["lugar"].str.len()
        df_tweets["empresa_start"] = df_tweets[~pd.isna(df_tweets["empresas"])].apply(
            lambda x: int(x["texto"].find(x["empresas"])), 
            axis=1
        )
        df_tweets["empresa_end"] = df_tweets["empresa_start"] + df_tweets["empresas"].str.len()
        df_tweets["tipo_lugar_start"] = df_tweets[~pd.isna(df_tweets["tipo_lugar"])].apply(
            lambda x: int(x["texto"].find(x["tipo_lugar"])), 
            axis=1
        )
        df_tweets["tipo_lugar_end"] = df_tweets["tipo_lugar_start"] + df_tweets["tipo_lugar"].str.len()
        
        df_tweets.loc[df_tweets["empresas"] == "", "empresas"] = np.nan
        df_tweets = df_tweets[~pd.isna(df_tweets["empresas"])]
        df_tweets["tipo_lugar"] = df_tweets["tipo_lugar"].fillna("None")
        df_tweets["lugar"] = df_tweets["lugar"].fillna("None")
        
        tweets_lugares = df_tweets.to_dict('records')
        
        for tweet in tweets_lugares:
            if (tweet["lugar"] != "None") and (tweet["tipo_lugar"] != "None"):
                #print(tweet, 'LUGAR Y TIPO LUGAR')
                TRAIN_DATA.append((
                    tweet["texto"], {
                        'entities': [
                            (int(tweet["lugar_start"]), int(tweet["lugar_end"]), 'LOC'),
                            (int(tweet["empresa_start"]), int(tweet["empresa_end"]), 'EMP')
                        ],
                        'cats':{tweet["tipo_lugar"].upper(): 0.9}
                    }
                ))
            elif (tweet["lugar"] != "None") and (tweet["tipo_lugar"] == "None"):
                #print(tweet, 'SOLO LUGAR')
                TRAIN_DATA.append((
                    tweet["texto"], {
                        'entities': [
                            (int(tweet["lugar_start"]), int(tweet["lugar_end"]), 'LOC'),
                            (int(tweet["empresa_start"]), int(tweet["empresa_end"]), 'EMP')
                        ],
                        'cats':{'COLOMBIA':1.0}
                    }
                ))
            else:
                #print(tweet, 'NINGUNO')
                TRAIN_DATA.append((
                    tweet["texto"], {
                        'entities': [
                            (int(tweet["empresa_start"]), int(tweet["empresa_end"]), 'EMP')
                        ],
                        'cats':{'COLOMBIA':1.0}
                    }
                ))
        
        print("CREATED TRAINING DATA", len(TRAIN_DATA), "TWEETS")
        return TRAIN_DATA

    def train_model(model="roxanne_model/", output_dir="roxanne_model", n_iter=500):
        """Load the model, set up the pipeline and train the entity recognizer."""
        if model is not None:
            nlp = spacy.load(model)  # Carga un modelo existente
            print("Carga modelo '%s'" % model)
        else:
            nlp = spacy.blank('es')  # Crea un modelo vacio
            print("Crear modelo vacio 'en' model")

        # Crea linea de accion para reconocimiento de labels y entidades
        if 'ner' not in nlp.pipe_names:
            ner = nlp.create_pipe('ner')
            nlp.add_pipe(ner, last=True)
        # Si existe, la obtiene para agregar los labels
        else:
            ner = nlp.get_pipe('ner')

        # Agrega los labels
        for _, annotations in TRAIN_DATA:
            for ent in annotations.get('entities'):
                ner.add_label(ent[2])

        # Excluye otras lineas de accion para solo usar NER
        other_pipes = [pipe for pipe in nlp.pipe_names if pipe != 'ner']
        with nlp.disable_pipes(*other_pipes):  # Unicamente entrene NER
            optimizer = nlp.begin_training()
            for itn in range(n_iter):
                random.shuffle(TRAIN_DATA)
                losses = {}
                for text, annotations in TRAIN_DATA:
                    nlp.update(
                        [text],  # Texto
                        [annotations],  # Anotaciones
                        drop=0.5,  # Ignora la mitad para dificultar el aprendizaje
                        sgd=optimizer,  # Actualiza los pesos
                        losses=losses)
                print(losses)
                
        if output_dir is not None:
            output_dir = Path(output_dir)
            if not output_dir.exists():
                output_dir.mkdir()
            nlp.to_disk(output_dir)
            print("Saved model to", output_dir)

    def test_example():
        nlp2 = spacy.load("roxanne_model/")

        for text in TEST_DATA:
            doc = nlp2(text)
            print('Entities', [(ent.text, ent.label_) for ent in doc.ents])
            print('Tokens', [(t.text, t.ent_type_, t.ent_iob) for t in doc])
