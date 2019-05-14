from datetime import date

from brand_safety.audit_models.base import Audit
from brand_safety.audit_models.brand_safety_channel_score import BrandSafetyChannelScore
from brand_safety import constants
from segment.models.persistent.constants import PersistentSegmentCategory


class BrandSafetyChannelAudit(object):
    """
    Brand safety Audit for channels using SDB data
    """
    failed_videos_count_threshold = 3
    brand_safety_metadata_threshold = 1
    channel_minimum_subscribers_whitelist = 1000

    def __init__(self, video_audits, audit_types, channel_data, **kwargs):
        self.source = kwargs["source"]
        self.score_mapping = kwargs["score_mapping"]
        self.brand_safety_score_multiplier = kwargs["brand_safety_score_multiplier"]
        self.default_category_scores = kwargs["default_category_scores"]
        self.auditor = Audit()
        self.results = {}
        self.video_audits = video_audits
        self.audit_types = audit_types
        self.metadata = self.get_metadata(channel_data)
        self.target_segment = None

    @property
    def pk(self):
        pk = self.metadata["channel_id"]
        return pk

    def run_audit(self):
        """
        Drives main audit logic
        :return:
        """
        for video in self.video_audits:
            self.results[constants.BRAND_SAFETY] = self.results.get(constants.BRAND_SAFETY, [])
            self.results[constants.BRAND_SAFETY].extend(video.results[constants.BRAND_SAFETY])
        title_hits = self.auditor.audit(self.metadata["channel_title"], constants.TITLE, self.audit_types[constants.BRAND_SAFETY])
        description_hits = self.auditor.audit(self.metadata["description"], constants.DESCRIPTION, self.audit_types[constants.BRAND_SAFETY])
        self.results["metadata_hits"] = title_hits + description_hits
        self.calculate_brand_safety_score(*title_hits, *description_hits)
        self.set_brand_safety_segment()

    def get_metadata(self, channel_data):
        """
        Set SDB metadata on audit for access of expected keys throughout runtime
        :param channel_data:
        :return: dict
        """
        text = channel_data.get("title", "") + channel_data.get("description", "")
        metadata = {
            "channel_id": channel_data.get("channel_id", ""),
            "channel_title": channel_data.get("title"),
            "channel_url": channel_data.get("url", ""),
            "language": channel_data.get("language", ""),
            "category": channel_data.get("category", ""),
            "description": channel_data.get("description", ""),
            "videos": channel_data.get("videos", constants.DISABLED),
            "subscribers": channel_data.get("subscribers", constants.DISABLED),
            "views": channel_data.get("video_views", 0),
            "audited_videos": len(self.video_audits),
            "has_emoji": self.auditor.audit_emoji(text, self.audit_types[constants.EMOJI]),
            "likes": channel_data.get("likes", constants.DISABLED),
            "dislikes": channel_data.get("dislikes", constants.DISABLED),
            "country": channel_data.get("country", ""),
            "thumbnail_image_url": channel_data.get("thumbnail_image_url", "")
        }
        return metadata

    def calculate_brand_safety_score(self, *channel_metadata_hits, **_):
        """
        Aggregate video audit brand safety results
        :param channel_metadata_hits: All channel metadata hits
        :return:
        """
        channel_brand_safety_score = BrandSafetyChannelScore(self.pk, len(self.video_audits), self.default_category_scores)
        # Aggregate video audits scores for channel
        for audit in self.video_audits:
            video_brand_safety_score = getattr(audit, constants.BRAND_SAFETY_SCORE)
            for keyword_name, data in video_brand_safety_score.keyword_scores.items():
                channel_brand_safety_score.add_keyword_score(keyword_name, data["category"], data["negative_score"], data["hits"])
            for category, score in video_brand_safety_score.category_scores.items():
                channel_brand_safety_score.add_category_score(category, score)
            channel_brand_safety_score.add_overall_score(video_brand_safety_score.overall_score)

        # Average all scores
        channel_brand_safety_score.calculate_average_scores()

        # Add brand safety metadata hits to scores, must be called after calculate_average_scores to
        # add weight against channel video averaged scores
        for word in channel_metadata_hits:
            try:
                multiplier = self.brand_safety_score_multiplier[word.location]
                keyword_category = self.score_mapping[word.name]["category"]
                keyword_score = self.score_mapping[word.name]["score"] * multiplier
                channel_brand_safety_score.add_metadata_score(word.name, keyword_category, keyword_score)
            except KeyError:
                pass
        setattr(self, constants.BRAND_SAFETY_SCORE, channel_brand_safety_score)
        return channel_brand_safety_score

    def instantiate_related_model(self, model, related_segment, segment_type=constants.WHITELIST):
        details = {
            "language": self.metadata["language"],
            "thumbnail": self.metadata["thumbnail_image_url"],
            "likes": self.metadata["likes"],
            "dislikes": self.metadata["dislikes"],
            "views": self.metadata["views"],
        }
        if segment_type == constants.BLACKLIST:
            details["bad_words"] = self.results["metadata_hits"]
        obj = model(
            related_id=self.pk,
            segment=related_segment,
            title=self.metadata["channel_title"],
            category=self.metadata["category"],
            details=details
        )
        return obj

    def es_repr(self, index_name, index_type, action):
        """
        ES Brand Safety Index expects documents formatted by category, keyword, and scores
        :return: ES formatted document
        """
        brand_safety_results = getattr(self, constants.BRAND_SAFETY_SCORE)
        brand_safety_es = {
            "_index": index_name,
            "_type": index_type,
            "_op_type": action,
            "_id": self.pk,
            "channel_id": brand_safety_results.pk,
            "overall_score": brand_safety_results.overall_score if brand_safety_results.overall_score >= 0 else 0,
            "videos_scored": brand_safety_results.videos_scored,
            "updated_at": str(date.today()),
            "categories": {
                category: {
                    "category_score": score,
                    "keywords": []
                }
                for category, score in brand_safety_results.category_scores.items()
            }
        }
        for _, data in brand_safety_results.keyword_scores.items():
            category = data.pop("category")
            brand_safety_es["categories"][category]["keywords"].append(data)
        return brand_safety_es

    def set_brand_safety_segment(self):
        """
        Sets attribute determining if audit should be part of master whitelist or blacklist
            If audit does not meet requirements for either whitelist or blacklist, then it should not be added to any segment
        :return:
        """
        # Immediately blacklist if any metadata hit
        if len(self.results["metadata_hits"]) >= self.brand_safety_metadata_threshold:
            self.target_segment = PersistentSegmentCategory.BLACKLIST
            return

        channel_subscribers = self.metadata["subscribers"] if self.metadata["subscribers"] is not constants.DISABLED else 0
        failed_video_audits = 0
        # Blacklist channel if number of blacklist videos exceeds threshold
        for audit in self.video_audits:
            if audit.target_segment == PersistentSegmentCategory.BLACKLIST:
                failed_video_audits += 1
            if failed_video_audits > self.failed_videos_count_threshold:
                self.target_segment = PersistentSegmentCategory.BLACKLIST
        # If channel has failed as this point, check subscribers
        if channel_subscribers > self.channel_minimum_subscribers_whitelist:
            self.target_segment = PersistentSegmentCategory.WHITELIST
        else:
            self.target_segment = None
