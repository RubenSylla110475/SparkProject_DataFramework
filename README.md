# Spark Data Framework / Streamlit Project  
**Ruben SYLLA ING 4 DATA IA Grp 03**

Ce projet est une application d'analyse avancée de données boursières, développée à l'aide de Spark et Streamlit.  
L'application permet d'importer et d'analyser des données historiques de divers titres (stocks) du Nasdaq, offrant ainsi des insights précieux pour la prise de décision en investissement.

---

## Présentation du Projet

L'objectif principal de cette application est de fournir une plateforme interactive qui permet :
- **L'importation des données** : Récupération des informations boursières via Yahoo Finance.
- **L'analyse statistique et financière** : Calcul d'indicateurs tels que les moyennes mobiles, les taux de rendement, la volatilité, le momentum, le ratio de Sharpe, le RSI, etc.
- **La visualisation interactive** : Affichage des graphiques et des tableaux de bord pour explorer les tendances et corrélations entre les actions.

Grâce à PySpark, l'application traite efficacement de grandes quantités de données, tandis que Streamlit offre une interface utilisateur intuitive et réactive.

---

## Comment c'est Fait ?

- **Langage** : Python

- **Technologies & Bibliothèques** :
  - **Streamlit** : Création d'une interface interactive pour l'analyse et la visualisation.
  - **PySpark** : Traitement distribué des données pour une meilleure performance.
  - **Pandas** : Manipulation et transformation des datasets.
  - **Matplotlib & Seaborn** : Visualisation graphique des indicateurs et tendances.
  - **yfinance** : Extraction des données boursières directement depuis Yahoo Finance.

- **Structure du Code** :
  Le script principal (lab2.py) réalise les étapes suivantes :
  - **Initialisation** : Démarrage d'une session Spark et configuration de l'environnement.
  - **Importation des données** : Téléchargement des données de plusieurs titres et conversion en DataFrame Spark.
  - **Analyses & Calculs** : Exécution de diverses fonctions pour :
    - Calculer des moyennes mobiles.
    - Analyser la distribution des prix.
    - Mesurer la corrélation entre les stocks.
    - Évaluer des indicateurs financiers (taux de rendement, volatilité, momentum, etc.).
  - **Visualisation** : Affichage interactif des résultats via Streamlit, avec des graphiques et des tableaux dynamiques.
  - **Paramétrage** : Utilisation d'un panneau latéral pour permettre à l'utilisateur de sélectionner les stocks, définir les dates d'analyse et ajuster d'autres paramètres.

---

## Résultats

Lors de l'exécution, l'application :
- **Présente un aperçu des données** : Affichage des premiers enregistrements et du schéma des données pour chaque titre sélectionné.
- **Visualise les analyses** :
  - Graphiques des distributions de prix et des moyennes mobiles.
  - Tableaux des statistiques descriptives et indicateurs financiers.
  - Visualisations interactives de la corrélation entre différentes actions.
- **Offre une analyse approfondie** : Permet d'explorer des mesures telles que le taux de rendement par période, la volatilité, la performance sectorielle, le momentum, et d'autres métriques clés qui aident à identifier les opportunités d'investissement.

Ces visualisations et résultats fournissent une compréhension détaillée des tendances du marché et aident à prendre des décisions éclairées basées sur des analyses quantitatives.

---

## Comment Lancer l'Application ?

1. **Installation des Dépendances**  
   Installez les bibliothèques requises via pip :

   ```bash
   pip install streamlit pandas yfinance matplotlib pyspark seaborn
Exécution de l'Application
Lancez l'application Streamlit avec la commande suivante :

bash
Copier
streamlit run app.py
Utilisation

Sélectionnez entre 2 et 5 stocks parmi ceux proposés.
Choisissez les dates de début et de fin pour l'analyse.
Configurez les paramètres d'analyse (période, mois, etc.) via le panneau latéral.
Cliquez sur Load Data pour importer et analyser les données, puis explorez les résultats interactifs affichés.
Ce projet démontre comment combiner la puissance de Spark pour le traitement des données et l'interactivité de Streamlit pour créer des applications d'analyse boursière robustes et intuitives.

![alt text](https://github.com/RubenSylla110475/SparkProject_DataFramework/blob/main/img/StreamLit_MainPage.jpg)
