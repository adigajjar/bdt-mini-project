from flask import Flask, render_template, request, redirect, url_for, jsonify
from pymongo import MongoClient
import random
from kafka import KafkaProducer
import json
from sklearn.decomposition import TruncatedSVD
import os
import librosa
from bson import ObjectId
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
import warnings
from flask_cors import CORS

warnings.filterwarnings("ignore")


def mongo_to_json(data):
    if isinstance(data, list):
        for item in data:
            item["_id"] = str(item["_id"])
    elif isinstance(data, dict):
        data["_id"] = str(data["_id"])
    return data


def convert_list_to_ndarray(data):
    if isinstance(data, dict):
        return {k: convert_list_to_ndarray(v) for k, v in data.items()}
    elif isinstance(data, list):
        return np.array(data)
    else:
        return data


audio_json_path = "audio_vectors.json"
lyrics_json_path = "lyrics_vectors.json"
with open(lyrics_json_path, "r") as f:
    data_lyrics = json.load(f)
lyrics_vectors = convert_list_to_ndarray(data_lyrics)

with open(audio_json_path, "r") as f:
    data_audio = json.load(f)
audio_vectors = convert_list_to_ndarray(data_audio)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

app = Flask(__name__)
CORS(app)
client = MongoClient("mongodb://localhost:27017/")
db = client["fsd_music"]
songs_collection = db["songs"]
temp_recs_collection = db["temp_recommendations"]


@app.route("/")
def home():
    songs = list(songs_collection.find().limit(7))
    return render_template("index.html", songs=songs)


def reduce_vector_size(vector, target_size):
    if len(vector) > target_size:
        svd = TruncatedSVD(n_components=target_size)
        vector = svd.fit_transform([vector])[0]
    return vector


def combine_vectors(audio_vector, lyrics_vector, alpha):
    # Reduce the lyrics vector size to match the audio vector size (13)
    lyrics_vector = reduce_vector_size(lyrics_vector, len(audio_vector))
    # Combine audio and lyrics vectors (weighted average)
    combined_vector = alpha * audio_vector + (1 - alpha) * lyrics_vector
    return combined_vector


# def get_recommendations(interacted_songs, audio_vectors, lyrics_vectors, alpha=0.5):
#     # Create a dictionary to hold the combined vectors of interacted songs
#     base_name = interacted_songs
#     target_audio_vector = audio_vectors.get(base_name)
#     target_lyrics_vector = lyrics_vectors.get(base_name)

#     if target_audio_vector is None or target_lyrics_vector is None:
#         raise KeyError(f"Missing data for target song: {base_name}")

#     target_vector = combine_vectors(target_audio_vector, target_lyrics_vector, alpha)

#     similarities = {}

#     for song, audio_vector in audio_vectors.items():
#         lyrics_vector = lyrics_vectors.get(song)
#         if song != base_name and lyrics_vector is not None:
#             combined_vector = combine_vectors(audio_vector, lyrics_vector, alpha)
#             sim = cosine_similarity([target_vector], [combined_vector])[0][0]
#             similarities[song] = sim

#     print(similarities.items())
#     recommended_songs = sorted(similarities.items(), key=lambda x: x[1], reverse=True)

#     # Return the top 5 recommended songs
#     rec = [song for song, _ in recommended_songs[1:7]]
#     rec_obj = list(songs_collection.find({"title": {"$in": rec}}))

#     return rec_obj


def get_recommendations(interacted_songs, audio_vectors, lyrics_vectors, alpha=0.5):
    final_recs = []
    for base_name in interacted_songs:
        target_audio_vector = audio_vectors.get(base_name)
        target_lyrics_vector = lyrics_vectors.get(base_name)

        if target_audio_vector is None or target_lyrics_vector is None:
            raise KeyError(f"Missing data for target song: {base_name}")

        target_vector = combine_vectors(
            target_audio_vector, target_lyrics_vector, alpha
        )

        similarities = {}

        for song, audio_vector in audio_vectors.items():
            lyrics_vector = lyrics_vectors.get(song)
            if song != base_name and lyrics_vector is not None:
                combined_vector = combine_vectors(audio_vector, lyrics_vector, alpha)
                sim = cosine_similarity([target_vector], [combined_vector])[0][0]
                similarities[song] = sim
        recommended_songs = sorted(
            similarities.items(), key=lambda x: x[1], reverse=True
        )
        rec = [song for song, _ in recommended_songs[1:10]]
        final_recs.extend(rec)

    print(final_recs)
    final_recs = random.sample(final_recs, 5)

    rec_obj = list(songs_collection.find({"title": {"$in": final_recs}}))
    temp_recs_collection.delete_many({})
    temp_recs_collection.insert_many(rec_obj)
    print(rec_obj)
    return rec_obj


@app.route("/get_recommendations")
def fetch_recommendations():
    recs = list(temp_recs_collection.find({}))
    return jsonify(recs)


interactions = {}


# @app.route("/song_click/<string:song_id>", methods=["POST"])
# def record_click(song_id):
#     user_id = request.remote_addr
#     print("Click detected!")

#     # Initialize the user interaction list if it doesn't exist
#     if user_id not in interactions:
#         interactions[user_id] = []
#         print(f"New user interaction initialized for user: {user_id}")

#     # Record interaction for the user
#     interactions[user_id].append(song_id)
#     print(f"User {user_id} interacted with song: {song_id}")
#     print(f"Current interactions: {interactions[user_id]}")

#     # Send the interaction to Kafka
#     producer.send("song_interactions", {"user_id": user_id, "song_id": song_id})
#     print(f"Sent interaction to Kafka: {song_id} by user {user_id}")

#     # Get recommendations based on the most recent interaction
#     recommended_songs = get_recommendations(
#         song_id, audio_vectors, lyrics_vectors, alpha=0.5
#     )

#     print(f"Recommended songs for user {user_id}: {recommended_songs}")

#     # Render the index.html with the recommended songs
#     return render_template("index.html", songs=recommended_songs)


@app.route("/song_click/<string:song_id>", methods=["POST"])
def record_click(song_id):
    user_id = request.remote_addr
    print("Click recorded:", song_id)

    if user_id not in interactions:
        interactions[user_id] = []

    interactions[user_id].append(song_id)
    print(f"User {user_id} interactions: {interactions[user_id]}")

    producer.send("song_interactions", {"user_id": user_id, "song_id": song_id})

    if len(interactions[user_id]) >= 4:
        print(
            f"User {user_id} has interacted with 3 songs. Fetching recommendations..."
        )
        recommended_songs = get_recommendations(
            interactions[user_id], audio_vectors, lyrics_vectors, alpha=0.5
        )
        print(recommended_songs)

        recommended_songs = mongo_to_json(recommended_songs)
        interactions[user_id] = []

        return jsonify({"recommended_songs": recommended_songs})

    return jsonify({"status": "interacted", "interactions": interactions[user_id]})


if __name__ == "__main__":
    app.run(debug=True, port=5005)
