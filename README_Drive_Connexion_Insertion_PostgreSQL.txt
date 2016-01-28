# CONNEXION à un google Drive / récupération des fichiers / insertion dans postgreSQL
# en python 3.4

# Avant de commencer:
	# Créer un credentials sur l’ API manager google developer
		# https://console.developers.google.com/apis/credentials?project=balmy-moonlight-119616&pli=1
	# Vérification de tous les packages nécessaires à l'exécution du code 


# 1 fichiers python:
	# DriveConnexion_insert_postgreSQL : code inspiré de https://developers.google.com/drive/v2/web/quickstart/python pour la connexion au google 		#drive et la récupération des fichiers  
	# MAIN:		Connexion à un google drive privée, lecture des fichiers page par page
			Connexion à PostgreSQL
			Création de la table velo'v
			Chargement des fichiers du 26/01 seulement et svg(pour tester)
			Insertion des fichiers dans la base
			Fin lorsqu'au moins 1380 fichiers ont été trouvé (dû au nombre de fichier à lire sur le drive)