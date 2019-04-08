from collections import Counter

import langid

from brand_safety import constants
from brand_safety.audit_models.base import Audit


class VideoAudit(Audit):
    dislike_ratio_audit_threshold = 0.2
    views_audit_threshold = 1000
    brand_safety_unique_threshold = 2
    brand_safety_hits_threshold = 3

    def __init__(self, data, audit_types, source=constants.YOUTUBE, score_mapping=None):
        self.audit_types = audit_types
        self.source = source
        self.metadata = self.get_metadata(data, source)
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

    def get_youtube_metadata(self, data):
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

    def get_sdb_metadata(self, data):
        """
        Extarct Single DB (SDB) Video object metadata
        :param data: SDB video object
        :return: Dictionary of formatted metadata
        """
        text = (data["title"] or "") + (data["description"] or "")
        metadata = {
            "channel_title": data.get("channelTitle", ""),
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
            "transcript": data.get("transcript") if data.get("transcript") is not None else ""
        }
        return metadata

    def run_custom_audit(self):
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

    def get_dislike_ratio(self):
        """
        Calculate Youtube dislike to like ratio
        :return:
        """
        likes = self.metadata["likes"] if self.metadata["likes"] is not constants.DISABLED else 0
        dislikes = self.metadata["dislikes"] if self.metadata["dislikes"] is not constants.DISABLED else 0
        try:
            ratio = dislikes / (likes + dislikes)
        except ZeroDivisionError:
            ratio = 0
        return ratio

    def run_standard_audit(self):
        brand_safety_audit = self.audit_types[constants.BRAND_SAFETY]
        hits = self.audit(brand_safety_audit)
        self.results[constants.BRAND_SAFETY] = hits
        self.calculate_brand_safety_score(self.score_mapping)

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

    def calculate_brand_safety_score(self, score_mapping):
        """
        Calculate brand safety score total and across categories
        :return: tuple -> (int) total score, (dict) scores by category
        """
        video_category_scores = {
            "video_id": self.metadata["video_id"],
            "overall_score": 0,
        }
        brand_safety_hits = self.results[constants.BRAND_SAFETY]
        counts = Counter(brand_safety_hits)
        for keyword, count in counts.items():
            # keyword_score = score_mapping[keyword]["score"] * count
            # TESTING
            keyword_score = 1 * count

            video_category_scores["overall_score"] += keyword_score
            counts[keyword] = {
                "hits": count,
                "score": keyword_score
            }
        video_category_scores["categories"] = counts
        setattr(self, constants.BRAND_SAFETY_SCORE, video_category_scores)
        return video_category_scores

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


