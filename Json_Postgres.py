import postgresql
import utils_Json_Postgres
from postgresql import exceptions
import time
import glob

####################################
## Déclaration des var utiles

# international resource identifier
IRI = 'pq://admin:admin@localhost:5432/'
# bd préalablement construite sur postgre
database_name = 'velov'
# nom de la table
table_name = 'velov'
# chemin du repertoire dans lequel sont enregistrés les json
repository = 'C:/Users/lata/PycharmProjects/LDSA_velov_JSON_to_SQL/drive/'
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


#################################
## Lancement du processus
def connect_postgres_insert(path, online_param):
    # Ouverture et connexion à la bd
    db = postgresql.open(IRI + database_name)

    # Création de la table 'velov'
    utils_Json_Postgres.create_table(db, sql_table, table_name, database_name)
    # Si la table existe déjà, il faut réouvrir la connexion
    if db.closed:
        db = postgresql.open(IRI + database_name)
    data, d = utils_Json_Postgres.data_create(path, online_param)
    # Déclaration  et préparation de la requête d'insertion
    sql_insert = utils_Json_Postgres.cons_insert(table_name, data)
    statement = None
    try:
        statement = db.prepare(sql_insert)
    except exceptions.DuplicateTableError:
        print("Une exception est soulevée!!! Erreur sur la requête d'insertion")

    # construction de la liste de valeurs à insérer dans l'ordre annoncée des VALUES
    utils_Json_Postgres.insertion(data, d, statement)


# EXEMPLE #
# connect_postgres_insert('C:/Users/lata/PycharmProjects/LDSA_velov_JSON_to_SQL/all.json', False)
# connect_postgres_insert('C:/Users/lata/PycharmProjects/LDSA_velov_JSON_to_SQL/ex.json', False)
# connect_postgres_insert('https://download.data.grandlyon.com/ws/rdata/jcd_jcdecaux.jcdvelov/all.json', True)
def main():
    start_time = time.time()
    i = 0
    f_test = glob.glob(repository+'*.json')
    nb_f = f_test.__len__()
    db = postgresql.open(IRI + database_name)
    # Création de la table 'velov'
    utils_Json_Postgres.create_table(db, sql_table, table_name, database_name)
    # Si la table existe déjà, il faut réouvrir la connexion
    # construction de la liste de valeurs à insérer dans l'ordre annoncée des VALUES
    while i<nb_f:
        print("******* fichier %s" %i)
        if db.closed:
            db = postgresql.open(IRI + database_name)
        data, d = utils_Json_Postgres.data_create(f_test[i], False)
        # Déclaration  et préparation de la requête d'insertion
        sql_insert = utils_Json_Postgres.cons_insert(table_name, data)
        statement = None
        try:
            statement = db.prepare(sql_insert)
        except exceptions.DuplicateTableError:
            print("Une exception est soulevée!!! Erreur sur la requête d'insertion")
        utils_Json_Postgres.insertion(data, d, statement)
        i += 1
    print("Tps execution--- %s seconds ---" % (time.time() - start_time))

if __name__ == '__main__':
    main()



