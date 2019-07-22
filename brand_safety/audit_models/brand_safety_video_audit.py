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
        # Try to get video language processor
        try:
            keyword_processor = brand_safety_audit[self.metadata["language"]]
        except KeyError:
            # Set the language the audit uses
            self.metadata["language"] = "all"
            keyword_processor = brand_safety_audit["all"]
        tag_hits = self.auditor.audit(self.metadata["tags"], constants.TAGS, keyword_processor)
        title_hits = self.auditor.audit(self.metadata["video_title"], constants.TITLE, keyword_processor)
        description_hits = self.auditor.audit(self.metadata["description"], constants.DESCRIPTION, keyword_processor)
        transcript_hits = self.auditor.audit(self.metadata["transcript"], constants.TRANSCRIPT, keyword_processor)
        self.results[constants.BRAND_SAFETY] = tag_hits + title_hits + description_hits + transcript_hits
        self.calculate_brand_safety_score(self.score_mapping, self.brand_safety_score_multiplier)

    def get_metadata(self, data):
        """
        Extract Single DB (SDB) Video object metadata
        :param data: SDB video object
        :return: Dictionary of formatted metadata
        """
        metadata = {
            "channel_title": data.get("channel__title", ""),
            "channel_url": "https://www.youtube.com/channel/" + data["channel_id"],
            "channel_subscribers": data.get("channel__subscribers", 0),
            "video_title": data.get("title", ""),
            "title": data.get("title", ""),
            "video_url": "https://www.youtube.com/video/" + data.get("video_id", ""),
            "views": data.get("views") if data.get("views") is not None else 0,
            "description": data.get("description", ""),
            "category": data.get("category").lower() if data.get("category") is not None else constants.UNKNOWN.lower(),
            "country": data.get("country", constants.UNKNOWN),
            "likes": data.get("likes", 0),
            "dislikes": data.get("dislikes", 0),
            "channel_id": data["channel_id"],
            "tags": data.get("tags", "") if data.get("tags") is not None else "",
            "video_id": data["video_id"],
            "transcript": data.get("transcript") if data.get("transcript") is not None else "",
            "thumbnail_image_url": data.get("thumbnail_image_url", ""),
        }
        text = ", ".join([
            metadata.get("video_title", ""),
            metadata.get("description", ""),
            metadata.get("tags", ""),
            metadata.get("transcript", ""),
        ])
        metadata["has_emoji"] = self.auditor.audit_emoji(text, self.audit_types[constants.EMOJI]),
        metadata["language"] = self.auditor.get_language(text)
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
        self.metadata["overall_score"] = brand_safety_score.overall_score
        self.metadata[constants.BRAND_SAFETY_HITS] = brand_safety_score.hits
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
            "channel_id": self.metadata["channel_id"],
            "title": self.metadata["video_title"],
            "overall_score": brand_safety_results.overall_score if brand_safety_results.overall_score >= 0 else 0,
            "language": self.metadata["language"],
            "youtube_category": self.metadata["category"],
            "views": self.metadata["views"],
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
