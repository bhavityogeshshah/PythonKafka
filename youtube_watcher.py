import logging
import sys
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer
import requests
from config import config
import json
from pprint import pformat


def fetch_playlist_item_page(googl_api_key, playlistId, page_token=None):
    response = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", params={
        "key" : googl_api_key,
        "playlistId" : playlistId,
        "part": "contentDetails",
        "pageToken": page_token,
    })
    payload = json.loads(response.text)
    logging.debug("Got %s", payload)
    return payload

def fetch_videos_page(googl_api_key, videoId, page_token=None):
    response = requests.get("https://www.googleapis.com/youtube/v3/videos", params={
        "key" : googl_api_key,
        "id" : videoId,
        "part": "snippet, statistics",
        "pageToken": page_token,
    })
    payload = json.loads(response.text)
    logging.debug("Got %s", payload)
    return payload

def fetch_playlist_item(googl_api_key, playlistId, page_token=None):
    payload = fetch_playlist_item_page(googl_api_key, playlistId, page_token)

    yield from payload["items"]

    next_page_token  = payload.get('nextPageToken')

    if next_page_token is not None:
        yield from fetch_playlist_item(googl_api_key, playlistId, next_page_token)


def fetch_videos(googl_api_key, playlistId, page_token=None):
    payload = fetch_videos_page(googl_api_key, playlistId, page_token)

    yield from payload["items"]

    next_page_token  = payload.get('nextPageToken')

    if next_page_token is not None:
        yield from fetch_videos(googl_api_key, playlistId, next_page_token)


def summarize_video(video):
    return {
        "video_id" : video["id"],
        "title" : video["snippet"]["title"],
        "views": int(video["statistics"].get("viewCount", 0)),
        "likes": int(video["statistics"].get("likes", 0)),
        "comments": int(video["statistics"].get("commentCount", 0)),
    }



def on_delivery(err, record):
    pass

def main():
    logging.info("Start")
    schema_registry_client = SchemaRegistryClient(config["schema_registry"])
    youtube_schema = schema_registry_client.get_latest_version("youtube_videos-value")
    kafka_config = config["kafka_config"] | {
        "key.serializer" : StringSerializer(),
        "value.serializer" : AvroSerializer(schema_registry_client, youtube_schema.schema.schema_str),
    }
    producer = SerializingProducer(kafka_config)
    googl_api_key = config.get('google_api_key')
    playlistId = config.get('youtube_playlist_id')
    for video_item in fetch_playlist_item(googl_api_key, playlistId):
        video_id = video_item['contentDetails']['videoId']
        for video in fetch_videos(googl_api_key, video_id):
            logging.info("Got %s", pformat(summarize_video(video)))

            producer.produce(
                topic='youtube_videos',
                key = video_id,
                value = {
                    "TITLE" : video["snippet"]["title"],
                    "VIEWS": int(video["statistics"].get("viewCount", 0)),
                    "LIKES": int(video["statistics"].get("likes", 0)),
                    "COMMENTS": int(video["statistics"].get("commentCount", 0)),
                },
                on_delivery = on_delivery
            )

    producer.flush()





if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())