# CONNEXION � postgreSQL et insertion de json en local
# python 3.4

# Pr�requis: t�l�chargement pr�alable des packages � importer

#Construction du code:
	# utils_Json_Postgres.py : d�finition de fonctions utiles � la cr�ation / insertion / connexion � la base PostGreSQL
		# create_table(): Cr�ation et v�rification de l'existance de la table
		# json_dict(data_json) : Conversion du Json en dictionnaire pour acc�der facilement aux champs nomm�s
		# conv_format(data_dict) : Conversions dans les bons formats: en integers/float/date : pour tous les int/float/date � d�tecter � la main
		# data_create(file_path_json, online, param_set_int, param_set_float, param_set_date) : Chargement et modification du Json
		# cons_insert(param_table_name, param_data) : construction de la requ�te d'insertion SQL
		# insertion(data_param, d_param, statement_param) : insertion des donn�es 
	# Json_Postgres.py : d�claration des param�tres n�cessaires pour la connexion � la table, la r�cup�ration des fichiers et la mise au bon format
		# main()

# Tps ex�cution : 0:08:29.634408 min pour 1281 fichiers et 21 colonnes

