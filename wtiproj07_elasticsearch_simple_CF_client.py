import wtiproj07_elasticsearch_simple_client


def get_preselected_movies_for_user(user_id, ec):
    # TODO new version from extended_client
    user_document = ec.get_movies_liked_by_user(user_id)
    movies_id = user_document['ratings']
    related_users_id = []
    related_movies_id = []
    for id in movies_id:
        movie_document = ec.get_users_that_like_movie(id)
        temp_users_id = movie_document['whoRated']
        related_users_id = list(set(related_users_id) | set(temp_users_id))
    for id in related_users_id:
        related_user_document = ec.get_movies_liked_by_user(id)
        temp_movies_id = related_user_document['rating']
        related_movies_id = list(set(related_movies_id) | set(temp_movies_id))
    related_movies_id = list(set(related_movies_id))
    return related_movies_id


def get_preselected_users_for_movie(movie_id, ec):
    # TODO new version from extended_client
    movie_document = ec.get_users_that_like_movie(movie_id)
    users_id = movie_document['whoRated']
    related_movies_id = []
    related_users_id = []
    for id in users_id:
        user_document = ec.get_movies_liked_by_user(id)
        temp_movies_id = user_document['rating']
        related_movies_id = list(set(related_movies_id) | set(temp_movies_id))
    for id in related_movies_id:
        related_movies_document = ec.get_users_that_like_movie(id)
        temp_users_id = related_movies_document['whoRated']
        related_users_id = list(set(related_users_id) | set(temp_users_id))
    related_users_id = list(set(related_users_id))
    return related_users_id


ec = wtiproj07_elasticsearch_simple_client.ElasticClient()
ec.index_documents()
related_movies_id = get_preselected_movies_for_user(75, ec)
related_users_id = get_preselected_users_for_movie(296, ec)