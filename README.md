# Pipeline de Données avec Apache Airflow

Ce projet configure un pipeline de données en utilisant Apache Airflow. Le pipeline inclut l'extraction, la transformation et le chargement (ETL) des données dans une base de données Postgres, ainsi que l'envoi d'un email de notification en cas de succès. Le DAG est conçu pour commencer à traiter les données à partir du 1er janvier 2024 et ajouter les nouvelles données manquantes à chaque exécution.

Ce pipeline est idéal pour la gestion et l'analyse des données climatiques et des jours fériés. Il extrait des informations météorologiques telles que les températures, ainsi que les coefficients et les jours fériés, afin de fournir des insights précieux pour divers domaines d'application comme la planification d'événements, la gestion énergétique, et d'autres domaines nécessitant une prévision précise basée sur des données climatiques et calendaires.

Nous utilisons des itérations pour récupérer les données (par exemple, `fetch_temperatures(max_iterations=15)`) car les données provenant des URL peuvent varier. Pour nous assurer d'avoir des données complètes et cohérentes pour chaque jour de l'année 2024, nous effectuons plusieurs itérations. Cela garantit que toutes les données nécessaires sont disponibles pour effectuer les jointures et les analyses ultérieures. Si les données récupérées sont insuffisantes, vous pouvez augmenter le nombre d'itérations.


## Prérequis

Avant de commencer, assurez-vous d'avoir installé les éléments suivants :

- Docker
- Docker Compose
- Un compte Gmail pour l'envoi des emails de notification

## Configuration

1. Clonez ce dépôt sur votre machine locale :

    ```bash
    git clone <URL_DU_DEPOT>
    cd <NOM_DU_DEPOT>
    ```

2. Modifiez le fichier `docker-compose.yaml` pour configurer les connexions et les volumes nécessaires.

    ```yaml
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./config:/opt/airflow/config
      - ./plugins:/opt/airflow/plugins
      - ./data:/opt/airflow/data
    ```

3. Mettez à jour la configuration SMTP dans `config/airflow.cfg` :

    ```ini
   [smtp]
   smtp_host = smtp.gmail.com
   smtp_starttls = True
   smtp_ssl = False
   smtp_user = votre_email@gmail.com
   smtp_password = votre_mot_de_passe
   smtp_port = 587
   smtp_mail_from = votre_email@gmail.com
    ```

## Lancement du projet

1. Initialisez la base de données Airflow :

    ```bash
    docker-compose up airflow-init
    ```

2. Lancez les services Docker avec Docker Compose :

    ```bash
    docker-compose up -d
    ```

3. Accédez à l'interface web d'Airflow via `http://localhost:8080`. Utilisez les identifiants par défaut (nom d'utilisateur : `airflow`, mot de passe : `airflow`).

4. Activez et exécutez le DAG nommé `data_pipeline`.

## Structure du projet

- `dags/`: Contient les fichiers DAG d'Airflow.
- `config/`: Contient les fichiers de configuration d'Airflow, y compris `airflow.cfg`.
- `data/`: Répertoire pour stocker les fichiers de données.
- `logs/`: Répertoire pour stocker les logs d'Airflow.
- `plugins/`: Répertoire pour les plugins personnalisés.

## Description du DAG

Le DAG `data_pipeline` suit les étapes suivantes :

1. `full_refresh`: Optionnellement, supprime les tables existantes dans Postgres pour un rafraîchissement complet.
2. `fetch_holidays`: Extrait et charge les données de vacances dans la table `holidays`.
3. `fetch_temperatures`: Extrait et charge les données de températures dans la table `temperatures`.
4. `fetch_coefficients`: Extrait et charge les coefficients dans la table `coefficients`.
5. `transform_data`: Transforme les données et les charge dans les tables cibles.
6. `send_mail`: Envoie un email de notification en cas de succès de l'exécution du DAG.

## Envoi d'Email de Notification

Le script utilise la fonction `send_mail` pour envoyer un email de notification via SMTP après l'exécution réussie du DAG. Assurez-vous que vos informations de connexion SMTP sont correctement configurées dans `airflow.cfg`.

## Dépannage

- Si vous rencontrez des problèmes de connexion SMTP, vérifiez les paramètres de sécurité de votre compte Gmail et autorisez les applications moins sécurisées.
- Pour vérifier les logs et les erreurs, consultez les fichiers dans le répertoire `logs/` ou utilisez l'interface web d'Airflow.

## Contribuer

Les contributions sont les bienvenues ! Pour toute suggestion ou amélioration, n'hésitez pas à ouvrir une issue ou à proposer un pull request.

## Licence

Ce projet est sous licence MIT. Consultez le fichier `LICENSE` pour plus de détails.

---

Merci d'avoir utilisé ce pipeline de données avec Apache Airflow. Pour toute question, contactez-moi à tachomk@gmail.com.
