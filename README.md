
# Université : ENSET Mohammedia

## Module : Big Data – Spark SQL

### Réalisé par : Bouchra RAFIK
### Encadré par : Abdelmajid BOUSSELHAM


Practical Activity: Spark SQL – Bike Sharing Analysis
1. Project Overview

This project analyzes a bike-sharing dataset using Apache Spark SQL.

2. Dataset Description

The dataset contains rental information such as users, stations, time, and price.

3. Data Loading and Exploration
   Schema of dataset
   ![Schema](images/schema.png)
   Sample records
   ![DisplayView](images/DisplayView.png)
   Total rentals
   ![TotalRentals](images/TotalRentals.png)
4. Revenue Analysis
   Total revenue
   ![TotalRevenue](images/TotalRevenue.png)
5. Station Analysis
   Rentals per station
   ![RentalsPerStation](images/RentalsPerStation.png)
   Top station
   ![StationWithHigherRentals](images/StationWithHigherRentals.png)
   Detailed station view
   ![RentalsEachStation](images/RentalsEachStation.png)
6. Time Analysis
   Hour extraction
   ![StartHour](images/StartHour.png)
   Rentals per hour
   ![RentalsPerhour](images/RentalsPerhour.png)
   Peak hours
   ![peakhours](images/peakhours.png)
   Popular stations (7–12h)
   ![PopularStartStation7-12](images/PopularStartStation7-12.png)
7. User Analysis
   Average age
   ![AvgAge](images/AvgAge.png)
   Gender distribution
   ![UserByGender](images/UserByGender.png)
   Age groups
   ![RentByAgeGroup](images/RentByAgeGroup.png)
8. Duration Analysis
   Average duration
   ![AverageDuration](images/AverageDuration.png)
9. Conclusion

This project highlights:

usage patterns
peak hours
station popularity
user behavior
revenue analysis

Ce que j’ai appris

À travers ce travail pratique sur Spark SQL, j’ai appris à :

- Charger et explorer un dataset réel avec Apache Spark
- Comprendre et analyser la structure des données avec `printSchema()`
- Explorer les données avec `show()` et `count()`
- Créer des vues temporaires SQL pour faciliter les requêtes
- Écrire et exécuter des requêtes SQL dans Spark
- Effectuer des agrégations avec `groupBy()` et les fonctions SQL
- Analyser le comportement des utilisateurs selon l’âge et le genre
- Identifier les heures de forte utilisation grâce aux fonctions temporelles
- Extraire des informations pertinentes à partir de grandes données
- Comprendre la popularité des stations et les performances du système

Ce TP m’a permis de mieux comprendre le traitement des Big Data avec Spark SQL et la transformation des données brutes en informations exploitables.