import json
from collections import defaultdict
from collections import Counter
import time

import langid

from brand_safety import constants
from brand_safety.audit_models.base import Audit


class CustomVideoAudit(Audit):
    dislike_ratio_audit_threshold = 0.2
    views_audit_threshold = 1000
    brand_safety_unique_threshold = 2
    brand_safety_hits_threshold = 3

    def __init__(self, data, audit_types, source=constants.YOUTUBE, score_mapping=None):
        self.audit_types = audit_types
        self.source = source
        self.metadata = self.get_metadata(data)
        self.results = {}
        self.score_mapping = score_mapping

    @property
    def pk(self):
        pk = self.metadata["video_id"]
        return pk

    def get_metadata(self, data):
        """
        Extract Youtube video metadata
        :param data: Youtube video data
        :return: Dictionary of formatted metadata
        """
        text = data["snippet"].get("title", "") + data["snippet"].get("description", "")
        metadata = {
            "channel_title": data["snippet"].get("channelTitle", ""),
            "channel_url": "https://www.youtube.com/channel/" + data["snippet"].get("channelId", ""),
            "channel_subscribers": data.get("statistics", {}).get("channelSubscriberCount"),
            "video_title": data["snippet"]["title"],
            "video_url": "https://www.youtube.com/video/" + data["id"],
            "has_emoji": self.detect_emoji(text),
            "views": data["statistics"].get("viewCount", constants.DISABLED),
            "description": data["snippet"].get("description", ""),
            "category": constants.VIDEO_CATEGORIES.get(data["snippet"].get("categoryId"), constants.DISABLED),
            "language": self.get_language(data),
            "country": data["snippet"].get("country", constants.UNKNOWN),
            "likes": data["statistics"].get("likeCount", constants.DISABLED),
            "dislikes": data["statistics"].get("dislikeCount", constants.DISABLED),
            "channel_id": data["snippet"].get("channelId", ""),
            "tags": data["snippet"].get("tags", []),
            "video_id": data["id"],
            "transcript": data["snippet"].get("transcript", ""),
        }
        return metadata

    def run_audit(self):
        """
        Executes audit method on all existing audit types
        :return:
        """
        for audit_type, regexp in self.audit_types.items():
            hits = self.audit(regexp)
            self.results[audit_type] = hits

    def get_language(self, data):
        """
        Parses metadata to detect language using langid
        :param data:
        :return:
        """
        language = data["snippet"].get("defaultLanguage", None)
        if language is None:
            text = data["snippet"].get("title", "") + data["snippet"].get("description", "")
            language = langid.classify(text)[0].lower()
        return language

    def get_export_row(self, audit_type=constants.BRAND_SAFETY):
        """
        Formats exportable csv row using object metadata
            Removes unused metadata before export
        :param audit_type:
        :return:
        """
        row = [
            self.metadata["channel_title"],
            self.metadata["channel_url"],
            self.metadata["channel_subscribers"],
            self.metadata["video_title"],
            self.metadata["video_url"],
            self.metadata["has_emoji"],
            self.metadata["views"],
            self.metadata["description"],
            self.metadata["category"],
            self.metadata["language"],
            self.metadata["country"],
            self.metadata["likes"],
            self.metadata["dislikes"],
            self.get_keyword_count(self.results[audit_type])
        ]
        return row
