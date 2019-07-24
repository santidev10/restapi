from datetime import date

from brand_safety.audit_models.base import Audit
from brand_safety.audit_models.brand_safety_channel_score import BrandSafetyChannelScore
from brand_safety import constants


class BrandSafetyChannelAudit(object):
    """
    Brand safety Audit for channels using SDB data
    """
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

    @property
    def pk(self):
        pk = self.metadata["channel_id"]
        return pk

    def run_audit(self):
        """
        Drives main audit logic
        :return:
        """
        brand_safety_audit = self.audit_types[constants.BRAND_SAFETY]
        for video in self.video_audits:
            self.results[constants.BRAND_SAFETY] = self.results.get(constants.BRAND_SAFETY, [])
            self.results[constants.BRAND_SAFETY].extend(video.results[constants.BRAND_SAFETY])
        # Try to get channel language processor
        try:
            keyword_processor = brand_safety_audit[self.metadata["language"]]
        except KeyError:
            # Set the language the audit uses
            self.metadata["language"] = "all"
            keyword_processor = brand_safety_audit["all"]
        title_hits = self.auditor.audit(self.metadata["channel_title"], constants.TITLE, keyword_processor)
        description_hits = self.auditor.audit(self.metadata["description"], constants.DESCRIPTION, keyword_processor)
        self.results["metadata_hits"] = title_hits + description_hits
        self.calculate_brand_safety_score(*title_hits, *description_hits)

    def get_metadata(self, channel_data):
        """
        Set SDB metadata on audit for access of expected keys throughout runtime
        :param channel_data:
        :return: dict
        """
        metadata = {
            "channel_id": channel_data.get("channel_id", ""),
            "channel_title": channel_data.get("title", ""),
            "title": channel_data.get("title", ""),
            "channel_url": channel_data.get("url", ""),
            "category": channel_data.get("category").lower() if channel_data.get("category") is not None else constants.UNKNOWN.lower(),
            "description": channel_data.get("description", ""),
            "videos": channel_data.get("videos", 0),
            "views": channel_data.get("views") if channel_data.get("views") is not None else 0,
            "audited_videos": len(self.video_audits),
            "likes": channel_data.get("likes", 0),
            "dislikes": channel_data.get("dislikes", 0),
            "country": channel_data.get("country", ""),
            "thumbnail_image_url": channel_data.get("thumbnail_image_url", ""),
            "tags": channel_data.get("tags", "") if channel_data.get("tags") is not None else "",
            "subscribers": channel_data.get("subscribers", 0),
            "monthly_views": channel_data.get("thirty_days_views", 0),
        }
        text = ", ".join([
            metadata["channel_title"],
            metadata["description"],
        ])
        metadata["has_emoji"] = self.auditor.audit_emoji(text, self.audit_types[constants.EMOJI]),
        metadata["language"] = self.auditor.get_language(text)

        if metadata["likes"] == 0 or metadata["dislikes"] == 0:
            metadata.update(self._aggregate_video_audit_data())
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
        self.metadata["overall_score"] = channel_brand_safety_score.overall_score
        self.metadata[constants.BRAND_SAFETY_HITS] = channel_brand_safety_score.hits
        return channel_brand_safety_score

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
            "title": self.metadata["channel_title"],
            "overall_score": brand_safety_results.overall_score if brand_safety_results.overall_score >= 0 else 0,
            "videos_scored": brand_safety_results.videos_scored,
            "updated_at": str(date.today()),
            "language": self.metadata["language"],
            "subscribers": self.metadata["subscribers"],
            "youtube_category": self.metadata["category"],
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

    def get_text_metadata(self):
        text = ", ".join([
            self.metadata.get("channel_title", ""),
            self.metadata.get("description", ""),
            self.metadata.get("tags", ""),
        ])
        return text

    def _aggregate_video_audit_data(self) -> dict:
        """
        Aggregate video audit data
        :return: dict
        """
        video_audits = self.video_audits
        aggregated_data = {
            "likes": 0,
            "dislikes": 0,
        }
        for video in video_audits:
            try:
                aggregated_data["likes"] += int(video.metadata["likes"])
            except ValueError:
                pass
            try:
                aggregated_data["dislikes"] += int(video.metadata["dislikes"])
            except ValueError:
                pass
        return aggregated_data
