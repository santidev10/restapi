from django.core.management.base import BaseCommand

from elasticsearch_dsl import Index

class Command(BaseCommand):
    help = 'Retrieve and audit comments.'

    def handle(self, *args, **kwargs):
        es_index = Index(
            name="video"
        )
        brand_safety_index_video_mapping = {
            "properties": {
                "video_id": {"type": "keyword"},
                "overall_score": {"type": "integer"},
                "categories": {
                    "dynamic": True,
                    "type": "nested",
                    "properties": {}
                }
            }
        }
        brand_safety_index_channel_mapping = {
            "properties": {
                "channel_id": {"type": "keyword"},
                "overall_score": {"type": "integer"},
                "videos_scored": {"type": "integer"},
                "categories": {
                    "dynamic": True,
                    "type": "nested",
                    "properties": {}
                }
            }
        }