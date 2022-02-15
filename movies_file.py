from pymongo import MongoClient
import json
from bson import ObjectId
try:
    connection = MongoClient('localhost', 27017)
except:
    print("Error in Connect")
db=connection['Mflix']
movies=db['movies']
#question 1 find top n movies
#question 4.b.1.(i)
def movies_with_highest_imdb_rating(n):
    output=movies.aggregate([{"$project": {"_id": 0,"title": 1, "imdb.rating": 1}},
                             {"$sort": {"imdb.rating": -1}}, {"$limit": n}])
    for show in output:
        print(show)
movies_with_highest_imdb_rating(5) #showing top 5 movies with the highest imdb rating
#question 4.b.1.(ii)
def movies_with_highest_imdbrating_in_givenyear(n,year):
    output=movies.aggregate(
        [{"$addFields":{"yr":{"$getField": {"field": {"$literal": "$numberInt"},"input":"$year"}},
        "rating":{"$getField":{"field":{"$literal":"$numberDouble"},"input": "$imdb.rating"}}}},
         {"$match":{"yr":{"$eq":year}}},{"$project":{"_id":0,"title":1,"yr":1,"rating":1}}, {"$sort":{"rating":-1}},{"$limit":n}])
    for show in output:
      print(show)
movies_with_highest_imdbrating_in_givenyear(5,'1989')#showing top 5 movies
#question 4.b.1.iii
def votes_greater_than_thousand(n):
    output=db.movies.aggregate([{"$addFields": {"vote": {"$getField": {"field" : {"$literal": "$numberInt"},"input": "$imdb.votes"}}}},
           {"$match": {"$expr": {"$gt": [{"$toInt": "$vote"}, 1000]}}},
           {"$sort": {"imdb.rating": -1}},
           {"$project": {"_id": 0,"title": 1, "imdb.rating": 1, "vote": 1}},
           {"$limit": n}])
    for show in output:
        print(show)
votes_greater_than_thousand(10) #showing 10 entries of votes over 1000
#question 4.b.1.iv
def tomato_rating(n,string_match):
    p=[{"$addFields": {"tomatoes_Rating":"$tomatoes.viewer.rating","result":{"$cond":{"if":{"$regexMatch":{"input":"$title",
        "regex":string_match}},"then":"yes","else":"no"}}}},{"$project":{"_id":0,"title":1,"tomatoes_Rating":1,"result":1}},
       {"$match":{"result":{"$eq":"yes"}}},{"$sort":{"tomatoes_Rating":-1}},{"$limit":n}]
    output=list(db.movies.aggregate(p))
    for show in output:
        print(show)
tomato_rating(5,'Land')
#question 4.b.2.(i)
def directors_created_maximum_movies(n):
    output= movies.aggregate([{"$unwind":"$directors"},{"$group":{"_id":{"dir_name" :"$directors"},
                             "Movie_count":{"$sum":1}}},
                             {"$project":{"dir_name":1,"Movie_count":1}},{"$sort":{"Movie_count": -1}}
                            ,{"$limit":n} ])
    for show in output:
       print(show)
directors_created_maximum_movies(5) # 5 directos who created max movies
#question 4.b.2.(ii)
def directors_created_maximum_movies_inyear(n,year):
  output=movies.aggregate([{"$addFields": {"yr":{"$getField":{"field":{"$literal":"$numberInt"},"input":"$year"}}}},
                           {"$unwind":"$directors"},{"$match":{"yr":{"$eq":year}}},{"$group":{"_id":{"director_name":"$directors"},"count":{"$sum":1}}},
                           {"$project":{"director_name":1,"count":1}},{"$sort":{"count":-1}},{"$limit":n}])
  for show in output:
      print(show)
directors_created_maximum_movies_inyear(5,'1990')#calling for 5 directors
#question 4.b.2.(iii)
def directors_with_highestMovies_in_givenGenre(n,genres):
    output=movies.aggregate([{"$unwind": "$directors"},{"$match":{"genres":{"$eq":genres}}},{"$group":{"_id":{"director_name":"$directors"},
                            "count":{"$sum":1}}},{"$project":{"director_name":1,"count":1}},{"$sort":{"count":-1}},{"$limit":n}])
    for show in output:
        print(show)
directors_with_highestMovies_in_givenGenre(5,'Short')
#question 4.b.3.(i)
def actors_in_maximum_number_OfMovies(n):
    output=movies.aggregate([{"$unwind":"$cast"},{"$group":{"_id":"$cast","count":{"$sum":1}}},
                             {"$sort":{"count":-1}},{"$limit":n}])
    for show in output:
     print(show)
actors_in_maximum_number_OfMovies(10)#top 10 actors appeared in maximum movies
#question 4.b.3.(ii)
def actors_maxMovies_for_anYear(n,year):
    output=movies.aggregate([{"$addFields":{"yr":{"$getField":{"field":{"$literal":"$numberInt"},"input":"$year"}}}},
                             {"$unwind":"$cast"},{"$match":{"yr":{"$eq":year}}},{"$group":{"_id":{"actor_name":"$cast"},"count":{"$sum":1}}},
                             {"$project":{"actor_name":1,"count":1}},{"$sort":{"count":-1}},{"$limit":n}])
    for show in output:
        print(show)
actors_maxMovies_for_anYear(10,'1990')
#question 4.b.3.(iii)
def actors_with_maximum_movie_for_a_Genre(n,genres):
    output=movies.aggregate([{"$unwind":"$cast"},{"$match":{"genres":{"$eq":genres}}},{"$group":{"_id":{"actor_name":"$directors"},
                             "count":{"$sum":1}}}, {"$project":{"actor_name":1,"count":1}},
                            {"$sort":{"count":-1}},{"$limit":n}])
    for show in output:
     print(show)
actors_with_maximum_movie_for_a_Genre(6, "Short")
#question 4.b.4
def top_movies_of_every_genre(n):
    output=movies.aggregate([{"$unwind":"$genres"},{"$sort":{"imdb.rating":-1}},{"$group":{"_id":"$genres","title":{"$push":"$title"},
                            "rating":{"$push":{"$getField":{"field":{"$literal":"$numberDouble"},"input":"$imdb.rating"}}}}},
                             {"$project":{"_id":1,"Movies":{"$slice":["$title",0,n]},
                                        "rating":{"$slice":["$rating",0,n]}}}])
    for show in output:
        print(show)
top_movies_of_every_genre(3)#top 3 movies of the genre with imdb rating
