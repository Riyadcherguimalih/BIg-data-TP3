
# TP 3 BIG DATA SPARK Base de donées de films

## Remerciements: 
Avant d'entamer ce rapport, Je proffite de l'occasion pour remercier  THEAIUniversity pour sa serie de 3 vidéos qui expliquent d'une manière simple comment permettre à Apache Spark(Pyspark) de fonctionner sur Anaconda 3
[The AI University](https://www.youtube.com/watch?v=XvbEADU0IPU&t=793s)

Je tiens à remercier aussi Datamaking pour sa tuto qui m'a aidé à me familiariser avec les différents requêtes de Pyspark
[Datamaking](https://www.youtube.com/watch?v=iZSa4RfqefQ)
## Introduction:

Apache Spark est un cadre populaire pour les grandes données. Il prend en charge une grande variété de tâches d'analyse de données, notamment le nettoyage des données, le traitement des flux et l'apprentissage machine. Il peut être utilisé pour effectuer des calculs de grande taille sur un ou plusieurs ordinateurs. Cela fait de lui un outil efficace lorsque les données deviennent trop volumineuses pour que les pandas puissent les traiter.

## 1 Essayer Pyspark
Dans le paragraphe suivant, je voulais juste tester quelques requêtes pour se fameliariser d'avantage avec Pyspark

La première étape consiste à charger les données donc nous allons charger deux fichiers CSV(movies.csv et ratings.csv) dans l'environnement. 

```Pyspark
C:\Users\shini_000> cdC:\Users\shini_000\OneDrive\Bureau\big data

C:\Users\shini_000\OneDrive\Bureau\big data>pyspark
```
![Load code](file:///C:/Users/shini_000/OneDrive/Bureau/big%20data/2.JPG)

Nous allons maintenant charger ces ensembles de données en utilisant les commandes suivantes dans le shell.
Schema est utilisé pour s'assurer que chaque colonne possède le type de données souhaité selon nos besoins. Pour cela, nous allons créer un StructType et y ajouter des StructFields avec le type de données visé.

Nous avons maintenant créé notre datafrme "moviesdf" sur laquelles nous effectuons des diverses transformations.
![3](file:///C:/Users/shini_000/OneDrive/Bureau/big%20data/3.JPG)
![4](file:///C:/Users/shini_000/OneDrive/Bureau/big%20data/4.JPG)

withColumn est utilisé pour créer une nouvelle colonne dans le dataframe avec une certaine condition.
La colonne des genres a plusieurs genres associés au même film or nous devons traiter chaque genre séparément ainsi nous allons diviser le genre avec l'opérateur "|" et ensuite utiliser Explode pour que chaque genre distinct soit dans sa propre rangée.


![5](file:///C:/Users/shini_000/OneDrive/Bureau/big%20data/5.JPG) 
Nous regroupons les films par genre et nous comptons le nombre de lignes pour savoir combien de films sont présents dans les différents genres.


Après avoir chargé ratingsdf de la même maniere que moviesdf, nous effectuons une jointure interne entre les deux dataframes

![9](file:///C:/Users/shini_000/OneDrive/Bureau/big%20data/9.JPG)


## 2  La requête écrite en PySPark 

### Lire la data

```python
ratings = spark.read.option("header", "true").csv("ratings.csv")

movies = spark.read.option("header", "true").csv("movies.csv")
```
Pour lire dans un fichier CSV, nous accédons à la classe DataFrameReader via read puis appel à la méthode csv()
movies.show() et read.show() nous permettrons de visualiser quelques lignes des Dataframes comme indiqué sur la capture d'écran suivante:
![11](file:///C:/Users/shini_000/OneDrive/Bureau/big%20data/11.JPG) 

### Principe de la requête:

Nous essaierons de répondre à la question, quels sont les films les plus polarisants? Ce sont les films qui divisent les opinions,càd les films où les gens ont tendance à les noter très bons ou très mals. Nous ne voulons considérer que les films qui obtiennent un certain nombre minimum de notes pour cela nous fixons un seuil de 300 notes.

Pour aborder cela, nous rechercherons les films avec l'écart type de classement le plus élevé. Il s'agit d'une mesure de la variation des données par rapport à la moyenne, donc dans ce cas, de la variation des notes d'un film autour de sa note moyenne. Un écart-type élevé suggérerait que les notes du film sont très variables.

```python
rate_dev = ratings\
.groupBy("movieId")\
.agg(count("userId").alias("num_ratings"), 
     avg(col("rating")).alias("avg_rating"),
     stddev(col("rating")).alias("std_rating")
    )\
.where("num_ratings > 300")
``` 
![12](file:///C:/Users/shini_000/OneDrive/Bureau/big%20data/12.JPG)

```python 
polirazing_movies = rate_dev.join(movies, ratings_stddev.movieId == movies.movieId)

polirazing_movies.sort(desc("std_rating")).show(20, truncate=False)
```
![13](file:///C:/Users/shini_000/OneDrive/Bureau/big%20data/13.JPG)

On voit que la liste des films polarisants comprend Pulp Fiction, Shawshankredemption et Forrest Gump





