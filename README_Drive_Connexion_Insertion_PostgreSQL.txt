# CONNEXION � un google Drive / r�cup�ration des fichiers / insertion dans postgreSQL
# en python 3.4

# Avant de commencer:
	# Cr�er un credentials sur l� API manager google developer
		# https://console.developers.google.com/apis/credentials?project=balmy-moonlight-119616&pli=1
	# V�rification de tous les packages n�cessaires � l'ex�cution du code 
	# Chargement de utils_Json_Postgres.py


# Code python:
	# DriveConnexion_insert_postgreSQL : code inspir� de https://developers.google.com/drive/v2/web/quickstart/python pour la connexion au google 				#drive et la r�cup�ration des fichiers  
	# MAIN:		Connexion � un google drive priv�e, lecture des fichiers page par page
			Connexion � PostgreSQL
			Cr�ation de la table velo'v
			Chargement des fichiers du 26/01 seulement et svg(pour tester)
			Insertion des fichiers dans la base
			Fin lorsqu'au moins 1380 fichiers ont �t� trouv� (d� au nombre de fichier � lire sur le drive)
	# utils_Json_Postgres.py : d�finition de fonctions utiles � la cr�ation / insertion / connexion � la base PostGreSQL 
-	# D�tail dans le README_Json_Postgres

# Tps d'ex�cution : 0:26:53.347988 min avec lecture de 2000 fichiers / chargement de 1380 json / insertion en bdd