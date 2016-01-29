# CONNEXION à postgreSQL et insertion de json en local
# python 3.4

# Prérequis: téléchargement préalable des packages à importer

#Construction du code:
	# utils_Json_Postgres.py : définition de fonctions utiles à la création / insertion / connexion à la base PostGreSQL
		# create_table(): Création et vérification de l'existance de la table
		# json_dict(data_json) : Conversion du Json en dictionnaire pour accéder facilement aux champs nommés
		# conv_format(data_dict) : Conversions dans les bons formats: en integers/float/date : pour tous les int/float/date à détecter à la main
		# data_create(file_path_json, online, param_set_int, param_set_float, param_set_date) : Chargement et modification du Json
		# cons_insert(param_table_name, param_data) : construction de la requête d'insertion SQL
		# insertion(data_param, d_param, statement_param) : insertion des données 
	# Json_Postgres.py : déclaration des paramètres nécessaires pour la connexion à la table, la récupération des fichiers et la mise au bon format
		# main()

# Tps exécution : 0:08:29.634408 min pour 1281 fichiers et 21 colonnes

