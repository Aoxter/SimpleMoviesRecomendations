import pandas as pd
from elasticsearch import Elasticsearch, helpers
import numpy as np


class ElasticClient:
    def __init__(self, address='localhost:10000'):
        self.es = Elasticsearch(address)
        # ------ Simple operations ------
    def index_documents(self):
        df = pd.read_csv('data/user_ratedmovies.dat', delimiter='\t').loc[:, ['userID', 'movieID', 'rating']]
        means = df.groupby(['userID'], as_index=False, sort=False).mean().loc[:, ['userID', 'rating']].rename(columns={'rating': 'ratingMean'})
        df = pd.merge(df, means, on='userID', how="left", sort=False)
        df['ratingNormal'] = df['rating'] - df['ratingMean']
        ratings = df.loc[:, ['userID', 'movieID', 'ratingNormal']].rename(columns={'ratingNormal': 'rating'}) \
            .pivot_table(index='userID', columns='movieID', values='rating').fillna(0)
        print("Indexing users...")
        index_users = [{
            "_index": "users",
            "_type": "user",
            "_id": index,
            "_source": { 'ratings': row[row > 0].sort_values(ascending=False).index.values.tolist() }
        } for index, row in ratings.iterrows()]
        helpers.bulk(self.es, index_users)
        print("Done")
        print("Indexing movies...")
        index_movies = [{
            "_index": "movies",
            "_type": "movie",
            "_id": column,
            "_source": { "whoRated": ratings[column][ratings[column] > 0].sort_values(ascending=False).index.values.tolist() }
        } for column in ratings]
        helpers.bulk(self.es, index_movies)
        print("Done")

    def get_movies_liked_by_user(self, user_id, index='users'):
        user_id = int(user_id)
        return self.es.get(index=index, doc_type="user", id=user_id)["_source"]

    def get_users_that_like_movie(self, movie_id, index='movies'):
        movie_id = int(movie_id)
        return self.es.get(index=index, doc_type="movie", id=movie_id)["_source"]

    def get_preselected_movies_for_user(self, user_id, index_users='users', index_movies='movies'):
        movies_id = self.es.search(index=index_users, body={"query": {"term": {"_id":user_id}}})['hits']['hits'][0]['_source']['ratings']
        related_users_docs = self.es.search(index=index_users, body={"query": {"terms": {"ratings": movies_id}}})['hits']['hits']
        related_movies_id = set()
        for ratings in related_users_docs:
            if ratings['_id'] != user_id:
                ratings = ratings['_source']['ratings']
                for rating in ratings:
                    if rating not in movies_id:
                        related_movies_id.add(rating)
        related_movies_id_2 = []
        for ratings in related_users_docs:
            if ratings['_id'] != user_id:
                ratings = ratings['_source']['ratings']
                related_movies_id_2 = list(set(related_movies_id_2) | set(ratings))
        return list(related_movies_id)

    def get_preselected_users_for_movie(self, movie_id, index_users='users', index_movies='movies'):
        users_id = self.es.search(index=index_movies, body={"query": {"term": {"_id": movie_id}}})['hits']['hits'][0]['_source'][
            'whoRated']
        related_movies_docs = self.es.search(index=index_movies, body={"query": {"terms": {"whoRated": users_id}}})['hits']['hits']
        related_users_id = set()
        for ratings in related_movies_docs:
            if ratings['_id'] != movie_id:
                ratings = ratings['_source']['whoRated']
                for rating in ratings:
                    if rating not in users_id:
                        related_users_id.add(rating)
        return list(related_users_id)

    def add_user_document(self, user_id, ratings, index_user='users', index_movie='movie'):
        self.es.create(index=index_user, doc_type="user", id=user_id, body={'ratings': ratings})
        for movie in ratings:
            if self.es.exists(index=index_movie, doc_type="movie", id=movie):
                users = self.get_users_that_like_movie(movie, index_movie)
                users = users["whoRated"]
                if user_id not in users:
                    users.append(user_id)
                    self.update_movie_document(movie, users, index_user, index_movie)
            else:
                users = []
                users.append(user_id)
                self.add_movie_document(str(movie), users, index_user, index_movie)

    def add_movie_document(self, movie_id, who_rated, index_user='users', index_movie='movie'):
        self.es.create(index=index_movie, doc_type="movie", id=movie_id, body={'whoRated': who_rated})
        for user in who_rated:
            if self.es.exists(index=index_user, doc_type="user", id=user):
                movies = self.get_movies_liked_by_user(user, index_user)
                movies = movies["ratings"]
                if movie_id not in movies:
                    movies.append(movie_id)
                    self.update_user_document(str(user), movies, index_user, index_movie)
            else:
                movies = []
                movies.append(movie_id)
                self.add_user_document(user, movies, index_user, index_movie)

    def update_user_document(self, user_id, ratings, index_user='users', index_movie='movie'):
        old_movies = self.get_movies_liked_by_user(user_id, index_user)
        old_movies = old_movies["ratings"]
        self.es.update(index=index_user, doc_type='user', id=user_id, body={'ratings': ratings})
        # updating movies by adding new user
        for movie in ratings:
            if movie in old_movies:
                old_movies.remove(movie)
            if self.es.exists(index=index_movie, doc_type="movie", id=movie):
                users = self.get_users_that_like_movie(movie, index_movie)
                users = users["whoRated"]
                if user_id not in users:
                    users.append(user_id)
                    self.update_movie_document(str(movie), users, index_user, index_movie)
            else:
                users = []
                users.append(user_id)
                self.add_movie_document(movie, users, index_user, index_movie)
        # updating movies by delete user
        for movie in old_movies:
            if self.es.exists(index=index_movie, doc_type="movie", id=str(movie)):
                users = self.get_users_that_like_movie(movie, index_movie)
                users = users["whoRated"]
                users.remove(user_id)
                self.update_movie_document(str(movie), users, index_user, index_movie)

    def update_movie_document(self, movie_id, who_rated, index_user='users', index_movie='movie'):
        old_users = self.get_users_that_like_movie(movie_id, index_movie)
        old_users = old_users["whoRated"]
        self.es.update(index=index_movie, doc_type="movie", id=movie_id, body={"whoRated": who_rated})
        # updating movies by adding new user
        for user in who_rated:
            if user in old_users:
                old_users.remove(user)
            if self.es.exists(index=index_user, doc_type="user", id=user):
                movies = self.get_movies_liked_by_user(user, index_user)
                movies = movies["ratings"]
                if movie_id not in movies:
                    movies.append(movie_id)
                    self.update_user_document(str(user), movies, index_user, index_movie)
            else:
                movies = []
                movies.append(movie_id)
                self.add_user_document(user, movies, index_user, index_movie)
        # updating movies by delete user
        for user in old_users:
            if self.es.exists(index=index_user, doc_type="user", id=str(user)):
                movies = self.get_movies_liked_by_user(user, index_user)
                movies = movies["ratings"]
                movies.remove(movie_id)
                self.update_user_document(str(user), movies, index_user, index_movie)

    def delete_user_document(self, user_id, index_user='users', index_movie='movie'):
        ratings = self.get_movies_liked_by_user(user_id, index_user)
        ratings = ratings["ratings"]
        self.es.delete(index=index_user, doc_type="user", id=user_id)
        for movie in ratings:
            if self.es.exists(index=index_movie, doc_type="movie", id=movie):
                users = self.get_users_that_like_movie(movie, index_movie)
                users = users["whoRated"]
                if user_id in users:
                    print("here")
                    users.remove(user_id)
                    self.update_movie_document(str(movie), users, index_user, index_movie, delete_flag=1)

    def delete_movie_document(self, movie_id, index_user='users', index_movie='movie'):
        who_Rated = self.get_users_that_like_movie(movie_id, index_movie)
        who_Rated = who_Rated["whoRated"]
        self.es.delete(index=index_movie, doc_type="movie", id=movie_id)
        for user in who_Rated:
            if self.es.exists(index=index_user, doc_type="user", id=user):
                movies = self.get_movies_liked_by_user(user, index_user)
                movies = movies["ratings"]
                if movie_id in movies:
                    movies.remove(movie_id)
                    self.update_user_document(str(user), movies, index_user, index_movie, delete_flag=1)

    def bulk_user_update(self, body, index):
        for dict in body:
            self.update_user_document(str(dict["user_id"]), dict["liked_movies"], index, "movies")

    def bulk_movie_update(self, body, index):
        for dict in body:
            self.update_movie_document(str(dict["movie_id"]), dict["users_who_liked_movie"], index, "users")


if __name__ == "__main__":
    ec = ElasticClient()
    # ec.index_documents()
    # ------ Simple operations ------
    ec.get_preselected_movies_for_user(75)