#
import io
import os
import httplib2
import googleapiclient
from googleapiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools
from oauth2client import file
import postgresql
import utils_Json_Postgres
from postgresql import exceptions
import time
import datetime
try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

############################################################
# Déclaration de variables nécessaire à l'exécution du code
###########################################################

SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'client_secret2.json'
APPLICATION_NAME = 'Drive API Python Quickstart'
# chemin de la branche principale pour la création du fichier credentials (droit d'accès au drive)
home_dir = 'C:/Users/lata/PycharmProjects/LDSA_velov_JSON_to_SQL/'
# chemin pour la création du fichier temporaire
outpath = 'C:/Users/lata/PycharmProjects/LDSA_velov_JSON_to_SQL/drive/'
format_f = 'json'
# international resource identifier
IRI = 'pq://admin:admin@localhost:5432/'
# bd préalablement construite sur postgre
database_name = 'velov'
# nom de la table
table_name = 'velov'
# script sql création de la table velov
sql_table = "CREATE TABLE {} (" \
            "id SERIAL PRIMARY KEY," \
            "number INTEGER NOT NULL," \
            "name VARCHAR(36) NOT NULL," \
            "address VARCHAR(65) NOT NULL," \
            "address2 VARCHAR(48)," \
            "commune VARCHAR(16) NOT NULL," \
            "nmarrond INTEGER," \
            "bonus VARCHAR(3) NOT NULL," \
            "pole VARCHAR(61) NOT NULL," \
            "lat DECIMAL(18) NOT NULL," \
            "lng DECIMAL(18) NOT NULL," \
            "bike_stands INTEGER NOT NULL," \
            "status VARCHAR(6) NOT NULL," \
            "available_bike_stands INTEGER NOT NULL," \
            "available_bikes INTEGER NOT NULL," \
            "availabilitycode INTEGER NOT NULL," \
            "availability VARCHAR(6) NOT NULL," \
            "banking VARCHAR(5) NOT NULL," \
            "gid INTEGER NOT NULL," \
            "last_update TIMESTAMP WITHOUT TIME ZONE NOT NULL," \
            "last_update_fme TIMESTAMP WITHOUT TIME ZONE NOT NULL)".format(table_name)


def get_credentials():
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir, 'drive-python-quickstart.json')

    store = oauth2client.file.Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else:
            credentials = tools.run(flow, store)

        print('Storing credentials to ' + credential_path)
    return credentials


# fonction principale de chargement et insertion du fichier dans la base
def dl_insert(param_results, param_s_f, param_outpath, param_format_f, param_IRI, param_database_name, param_table_name):
    start_time = time.time()
    db = postgresql.open(param_IRI + param_database_name)
    nb_files = 0
    items = param_results.get('files', [])
    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            print('{0} ({1})'.format(item['name'], item['id']))
            file_id = item['id']
            file_name = item['name']
            # filtrage sur le 26/01
            if file_name[0:10] == '26/01/2016':
                nb_files += 1
                print("*******Chargement et traitement du fichier******", file_name)
                file_name = file_name.replace("/", "_").replace(" ", "_").replace(":", "_")
                # chargement get_media
                request = param_s_f.get_media(fileId=file_id)
                outfilename = param_outpath+file_name+"."+param_format_f
                outfile = io.FileIO(outfilename, mode='w+')
                downloader = googleapiclient.http.MediaIoBaseDownload(outfile, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                if db.closed:
                    print("!!Connexion réouverte!!")
                    db = postgresql.open(param_IRI + param_database_name)
                data, d = utils_Json_Postgres.data_create(outfilename, False)
                # Déclaration  et préparation de la requête d'insertion
                sql_insert = utils_Json_Postgres.cons_insert(param_table_name, data)
                statement = None
                try:
                    statement = db.prepare(sql_insert)
                except exceptions.DuplicateTableError:
                    print("Une exception est soulevée!!! Erreur sur la requête d'insertion")
                utils_Json_Postgres.insertion(data, d, statement)
    print("Tps execution pour le chargement et l'insertion des %s fichiers" % nb_files)
    print(" --- %s seconds ---" % (time.time() - start_time))
    return nb_files


def main():
    start_time = time.time()
    nb_files_tot = 0
    # Ouverture et connexion à la bd
    db = postgresql.open(IRI + database_name)
    # Création de la table 'velov'
    utils_Json_Postgres.create_table(db, sql_table, table_name, database_name)
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v3', http=http, credentials=credentials)
    s_f = service.files()
    results = s_f.list(pageSize=1000, fields="nextPageToken, files(id, name)").execute()
    # id de la page suivante pour n'oublier aucun fichier
    n_p_t = results.get('nextPageToken')
    nb_files_tot = nb_files_tot + dl_insert(results, s_f, outpath, format_f, IRI, database_name,table_name)
    while n_p_t is not None and nb_files_tot < 1282:
        print("************Nouvelle page du Drive**************")
        results = s_f.list(pageSize=1000, pageToken = n_p_t, fields="nextPageToken, files(id, name)").execute()
        # id de la page suivante pour n'oublier aucun fichier
        n_p_t = results.get('nextPageToken')
        nb_files_tot = nb_files_tot + dl_insert(results, s_f, outpath, format_f, IRI, database_name,table_name)
    print("nb tot: %s" % nb_files_tot)
    tmp = time.time() - start_time
    tmp = str(datetime.timedelta(seconds=tmp))
    print(" Fin du script en --- %s  ---" % tmp)


if __name__ == '__main__':
    main()