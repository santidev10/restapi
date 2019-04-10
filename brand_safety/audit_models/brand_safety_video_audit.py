import json
from collections import defaultdict
from collections import Counter
import time

import langid

from brand_safety import constants
from brand_safety.audit_models.base import Audit


class BrandSafetyVideoAudit(Audit):
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

    def instantiate_related_model(self, model, related_segment, segment_type=constants.WHITELIST):
        details = {
            "language": self.metadata["language"],
            "thumbnail": self.metadata["thumbnail_image_url"],
            "likes": self.metadata["likes"],
            "dislikes": self.metadata["dislikes"],
            "views": self.metadata["views"],
        }
        if segment_type == constants.BLACKLIST:
            details["bad_words"] = self.results[constants.BRAND_SAFETY]
        obj = model(
            related_id=self.pk,
            segment=related_segment,
            title=self.metadata["video_title"],
            category=self.metadata["category"],
            details=details
        )
        return obj

    def get_metadata(self, data):
        """
        Extarct Single DB (SDB) Video object metadata
        :param data: SDB video object
        :return: Dictionary of formatted metadata
        """
        text = data.get("title", "") + data.get("description", "")
        metadata = {
            "channel_title": data.get("channel__title", ""),
            "channel_url": "https://www.youtube.com/channel/" + data["channel_id"],
            "channel_subscribers": data.get("statistics", {}).get("channelSubscriberCount"),
            "video_title": data.get("title", ""),
            "video_url": "https://www.youtube.com/video/" + data.get("video_id", ""),
            "has_emoji": self.detect_emoji(text),
            "views": data.get("views", 0),
            "description": data.get("description", ""),
            "category": data.get("category", ""),
            "language": data.get("language", ""),
            "country": data.get("country", constants.UNKNOWN),
            "likes": data.get("likes", 0),
            "dislikes": data.get("dislikes", 0),
            "channel_id": data.get("channel_id", ""),
            "tags": data.get("tags", []),
            "video_id": data["video_id"],
            "transcript": data.get("transcript") if data.get("transcript") is not None else "",
            "thumbnail_image_url": data.get("thumbnail_image_url", "")
        }
        return metadata

    def run_audit(self):
        brand_safety_audit = self.audit_types[constants.BRAND_SAFETY]
        hits = self.audit(brand_safety_audit)
        self.results[constants.BRAND_SAFETY] = hits
        self.calculate_brand_safety_score(self.score_mapping)

    def get_text(self):
        metadata = self.metadata
        text = ""
        text += metadata["video_title"]
        text += metadata["description"]
        text += metadata["channel_title"]
        text += ",".join(metadata["tags"]) if metadata.get("tags") else ""
        # transcript value may actually be stored as None
        text += metadata["transcript"] if metadata["transcript"] is not None else ""
        return text

    def calculate_brand_safety_score(self, score_mapping):
        """
        Calculate brand safety score total and across categories
        :return: tuple -> (int) total score, (dict) scores by category
        """
        brand_safety_score = {
            "video_id": self.metadata["video_id"],
            "overall_score": 0,
            "keywords": defaultdict(dict)
        }
        brand_safety_hits = self.results[constants.BRAND_SAFETY]
        counts = Counter(brand_safety_hits)
        for keyword_name, count in counts.items():
            try:
                keyword_category = score_mapping[keyword_name]["category"]
                keyword_score = score_mapping[keyword_name]["score"]
                score = keyword_score * count
                brand_safety_score["overall_score"] += score
                brand_safety_score["keywords"][keyword_name] = {
                    "category": keyword_category,
                    "hits": count,
                    "score": score
                }
            except KeyError:
                print(keyword_name)
        setattr(self, constants.BRAND_SAFETY_SCORE, brand_safety_score)
        return brand_safety_score

    def es_repr(self):
        """
        ES Brand Safety Index expects documents formatted by category, keyword, and scores
            Video brand safety results must be formatted since they are processed by keyword, not by category
        :return: ES formatted document
        """
        brand_safety_results = getattr(self, constants.BRAND_SAFETY_SCORE)
        brand_safety_es_repr = {
            "video_id": brand_safety_results["video_id"],
            "overall_score": brand_safety_results["overall_score"],
            "categories": defaultdict(dict)
        }
        for keyword_name, data in brand_safety_results["keywords"].items():
            category = data.pop("category")
            brand_safety_es_repr["categories"][category][keyword_name] = data
        return brand_safety_es_repr

    # def run_standard_audit(self):
    #     brand_safety_counts = self.results.get(constants.BRAND_SAFETY)
    #     brand_safety_failed = brand_safety_counts \
    #                           and (
    #                                   len(brand_safety_counts.keys() >= self.brand_safety_hits_threshold)
    #                                   or any(
    #                               brand_safety_counts[keyword] > self.brand_safety_hits_threshold for keyword in
    #                               brand_safety_counts)
    #                           )
    #     dislike_ratio = self.get_dislike_ratio()
    #     views = self.metadata["views"] if self.metadata["views"] is not constants.DISABLED else 0
    #     failed_standard_audit = dislike_ratio > self.dislike_ratio_audit_threshold \
    #                             and views > self.views_audit_threshold \
    #                             and brand_safety_failed
    #     return failed_standard_audit