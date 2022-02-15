from pymongo import MongoClient
import json
from bson import ObjectId
try:
    connection = MongoClient('localhost', 27017)
    print("connected successfully")
except:
    print("Error in Connect")
#creating database or extracting old one
db = connection["Mflix"]
comments=db['comments']
#question 4.a.(i):find top 10 users who made maximum number of comments
def top_ten_user_max_comments(n):
   output=comments.aggregate([{"$group":{"_id":{"name": "$name"},"total_comments":{"$sum": 1}}},
                             {"$sort":{"total_comments": -1}},
                             {"$limit": n}])
   for show in output:
    print(show)
top_ten_user_max_comments(10)
#question 4.a.(ii): find top 10 movies with most comments
def top_ten_movies_having_most_comment(n):
    output = comments.aggregate([{"$group": {"_id": {"name": "$movie_id"}, "total_comments": {"$sum": 1}}},
                               {"$sort": {"total_comments": -1}},
                                {"$limit": n}])
    for show in output:
        print(show)
top_ten_movies_having_most_comment(10)
#question 4.a.(iii):given a year find the total number of comments created each month in that year
def comment_for_each_month_of_year(year):
  output = comments.aggregate([{"$project": {"_id": 0, "date":{"$toDate":{"$convert":{"input":"$date","to":"long"}}}}},
                              {"$group":{"_id":{"year":{"$year":"$date"},"month":{"$month":"$date"}},"total_comment":{"$sum": 1}}},
                               {"$match":{"_id.year":{"$eq":year}}},
                                {"$sort":{"_id.month": 1}}])
  for show in output:
        print(show)
comment_for_each_month_of_year(1978)
