from collections import defaultdict

from brand_safety import constants
from brand_safety.audit_models.base import Audit
from brand_safety.audit_models.brand_safety_video_score import BrandSafetyVideoScore


class BrandSafetyVideoAudit(object):
    def __init__(self, data, audit_types, **kwargs):
        self.source = kwargs["source"]
        self.brand_safety_score_multiplier = kwargs["brand_safety_score_multiplier"]
        self.score_mapping = kwargs["score_mapping"]
        self.default_category_scores = kwargs["default_category_scores"]
        self.languages = kwargs["languages"]
        self.auditor = Audit()
        self.audit_types = audit_types
        self.metadata = self.get_metadata(data)
        self.results = defaultdict(list)

    @property
    def pk(self):
        pk = self.metadata["video_id"]
        return pk

    def run_audit(self):
        brand_safety_audit = self.audit_types[constants.BRAND_SAFETY]
        language_code = self.languages[self.metadata["language"].lower()]
        # Try to get video language processor
        try:
            keyword_processor = brand_safety_audit[language_code]
        except KeyError:
            keyword_processor = brand_safety_audit["all"]
        tag_hits = self.auditor.audit(self.metadata["tags"], constants.TAGS, keyword_processor)
        title_hits = self.auditor.audit(self.metadata["video_title"], constants.TITLE, keyword_processor)
        description_hits = self.auditor.audit(self.metadata["description"], constants.DESCRIPTION, keyword_processor)
        transcript_hits = self.auditor.audit(self.metadata["transcript"], constants.TRANSCRIPT, keyword_processor)
        self.results[constants.BRAND_SAFETY] = tag_hits + title_hits + description_hits + transcript_hits
        self.calculate_brand_safety_score(self.score_mapping, self.brand_safety_score_multiplier)

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
            "has_emoji": self.auditor.audit_emoji(text, self.audit_types[constants.EMOJI]),
            "views": data.get("views", 0),
            "description": data.get("description", ""),
            "category": data.get("category") if data.get("category") is not None else "",
            "language": data.get("language") if data.get("language") is not None else "",
            "country": data.get("country", constants.UNKNOWN),
            "likes": data.get("likes", 0),
            "dislikes": data.get("dislikes", 0),
            "channel_id": data.get("channel_id", ""),
            "tags": data.get("tags", ""),
            "video_id": data["video_id"],
            "transcript": data.get("transcript") if data.get("transcript") is not None else "",
            "thumbnail_image_url": data.get("thumbnail_image_url", "")
        }
        return metadata

    def calculate_brand_safety_score(self, score_mapping, multiplier_ref):
        """
        Calculate brand safety score total and across categories
        :return: tuple -> (int) total score, (dict) scores by category
        """
        brand_safety_score = BrandSafetyVideoScore(self.pk, self.default_category_scores)
        brand_safety_hits = self.results[constants.BRAND_SAFETY]
        for word in brand_safety_hits:
            multiplier = multiplier_ref.get(word.location, 1)
            try:
                keyword_category = score_mapping[word.name]["category"]
                keyword_score = score_mapping[word.name]["score"]
                calculated_score = keyword_score * multiplier
                brand_safety_score.add_keyword_score(word.name, keyword_category, calculated_score)
            except KeyError:
                pass
        setattr(self, constants.BRAND_SAFETY_SCORE, brand_safety_score)
        return brand_safety_score

    def es_repr(self, index_name, index_type, op_type):
        """
        ES Brand Safety Index expects documents formatted by category, keyword, and scores
        :return: ES formatted document
        """
        brand_safety_results = getattr(self, constants.BRAND_SAFETY_SCORE)
        brand_safety_es = {
            "_index": index_name,
            "_type": index_type,
            "_op_type": op_type,
            "_id": self.pk,
            "video_id": brand_safety_results.pk,
            "overall_score": brand_safety_results.overall_score if brand_safety_results.overall_score >= 0 else 0,
            "categories": {
                category: {
                    "category_score": category_score,
                    "keywords": []
                }
                for category, category_score in brand_safety_results.category_scores.items()
            }
        }
        for keyword, data in brand_safety_results.keyword_scores.items():
            category = data.pop("category")
            brand_safety_es["categories"][category]["keywords"].append(data)
        return brand_safety_es
