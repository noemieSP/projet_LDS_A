import postgresql
from postgresql import exceptions
import json
from datetime import datetime
from urllib.request import urlretrieve

##################################################################################
# Déclaration des fonctions utiles


# Création et vérification de l'existance de la table
# Paramètres:   param_db : sortie de l'ouverture de la connexion
#               param_sql_table: appel à la "create table" déclarée par l'utilisateur
#               param_table_name: nom de la table déclarée par l'utilisateur
#               param_database_name: nom de la database déclarée par l'utilisateur
def create_table(param_db, param_sql_table, param_table_name, param_database_name):
    try:
        exe = param_db.execute(param_sql_table)
    except postgresql.exceptions.DuplicateTableError:
        print("La table « {}.{} » existe déjà.".format(param_database_name, param_table_name))
    else:
        if exe is None:
            print("db.execute('{}') n'a pas renvoyé d'erreur.".format(param_sql_table))


# Conversion Json en dictionnaire pour accéder facilement aux champs nommés
# Paramètres:   data_json: data json déjà chargé
# Sortie: data json transformé en dictionnaire
def json_dict(data_json):
    if isinstance(data_json['values'][0], list):
        print('[load_json] Conversion list -> dict')
        val_list = list()
        l = 0
        while l < data_json['values'].__len__():
            dico = dict()
            for index in range(data_json['fields'].__len__()):
                dico[data_json['fields'][index]] = data_json['values'][l][index]
            val_list.append(dico)
            l += 1
        data_json['values'] = val_list
    res = data_json['values']
    return res


# Conversions dans les bons formats: en integers / float / date : pour tous les int / float / date à détecter à la main
# Paramètres:   data_dict : data json ayant déjà été transformé en dictionnaire
#               param_set_int : set déclaré, comprenant les champs integer du json
#               param_set_float : set déclaré, comprenant les champs float du json
#               param_set_date : set déclaré, comprenant les champs de type du json
# Sortie: data_dict avec les bons formats
def conv_format(data_dict, param_set_int, param_set_float, param_set_date):
    for i in range(data_dict.__len__()):
        for key in param_set_float:
            data_dict[i][key] = float(data_dict[i][key])
        for key in param_set_int:
            try:
                data_dict[i][key] = int(data_dict[i][key])
            except ValueError as val_err:
                data_dict[i][key] = None
        for key in param_set_date:
            data_dict[i][key] = datetime.strptime(data_dict[i][key], "%Y-%m-%d %H:%M:%S")
    return data_dict


# Chargement et modification du Json
# Parmètres:    file_path_json: url/chemin du json
#               online: booleen, True si le json est en ligne / False s'il est en local
# Sortie : data d
def data_create(file_path_json, online, param_set_int, param_set_float, param_set_date):
    if online:
        file_path_json, header = urlretrieve(file_path_json)
    json_data = open(file_path_json, mode='r')
    data = json.load(json_data)
    # Conversion en dictionnaire puis typage
    d = json_dict(data)
    d = conv_format(d, param_set_int, param_set_float, param_set_date)
    print("Fichier chargé et modifié avec succès")
    return data, d


# Construction de l'INSERT SQL
# Paramètres:   param_table_name: table SQL déjà créée
#               param_data: data json préalablement chargé
# Sortie : statement nécessaire à chaque appel d'insertion dans la table
def cons_insert(param_table_name, param_data):
    res = "INSERT INTO {} (".format(param_table_name)
    # parcours des champs déclarés dans fields
    for field in param_data['fields']:
        res += field + ", "
    res = res[:-2] + ") VALUES ("
    for indice in range(1, param_data['fields'].__len__() + 1):
        res += "${},".format(indice)
    res = res[:-1] + ")"
    print("Construction de la requête d'insertion")
    return res


# Insertion
# Paramètres:   data_param: data chargée comprenant encore les fields
#               d_param: json modifiée (mise en format)
#               statement_param: statement construit par la fonction cons_insert
def insertion(data_param, d_param, statement_param):
    c_ligne = 0
    for i in range(d_param.__len__()):
        li = list()
        for key in data_param['fields']:
            li.append(d_param[i][key])
        statement_param(*li)
        c_ligne += 1
    print("Insertion des {} entrées pour {} lignes du json".format(c_ligne, d_param.__len__()))