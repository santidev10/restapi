from brand_safety import constants
from brand_safety.audit_models.brand_safety_channel_score import BrandSafetyChannelScore
from es_components.models import Channel


class BrandSafetyChannelAudit(object):
    """
    Brand safety Audit for channels using SDB data
    """
    metadata = {}
    score_multiplier = {
        "title": 4,
        "description": 1,
        "tags": 1,
        "transcript": 1
    }

    def __init__(self, channel_data, audit_utils, blacklist_data=None):
        self.video_audits = channel_data["video_audits"]
        self.audit_utils = audit_utils
        self.score_mapping = audit_utils.score_mapping
        # If channel has video audits, then channel should start with default_zero_score to find average category scores
        # Else, channel should start with full_score since no average can be calculated and will be used to subtract metadata scores
        self.default_category_scores = audit_utils.default_zero_score if len(self.video_audits) > 0 else audit_utils.default_full_score
        self.language_processors = audit_utils.bad_word_processors_by_language
        self._set_metadata(channel_data)
        self.blacklist_data = blacklist_data or {}

    @property
    def pk(self):
        pk = self.metadata["id"]
        return pk

    def _set_metadata(self, data: dict):
        """
        Set audit metadata
        :param data: keys = channel_id, description, channel_title
        :return:
        """
        text = ", ".join([
            data.get("title", "") or "",
            data.get("description", "") or "",
            data.get("video_tags", "") or "",
            ])
        detected = {
            "has_emoji": self.audit_utils.has_emoji(text),
            "language": self.audit_utils.get_language(text)
        }
        data.update(detected)
        self.metadata = data

    def run(self):
        """
        Call all required audit methods here
        :return:
        """
        self._run_brand_safety_audit()

    def _run_brand_safety_audit(self):
        """
        Brand safety audit
        :return:
        """
        # Try to get channel language processor
        try:
            keyword_processor = self.language_processors[self.metadata["language"]]
        except KeyError:
            # Set the language the audit uses
            self.metadata["language"] = "all"
            keyword_processor = self.language_processors["all"]
        title_hits = self.audit_utils.audit(self.metadata["title"], constants.TITLE, keyword_processor)
        description_hits = self.audit_utils.audit(self.metadata["description"], constants.DESCRIPTION, keyword_processor)

        score = self.calculate_brand_safety_score(*title_hits, *description_hits)
        setattr(self, constants.BRAND_SAFETY_SCORE, score)

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
                multiplier = self.score_multiplier[word.location]
                keyword_category = self.score_mapping[word.name]["category"]
                keyword_score = self.score_mapping[word.name]["score"] * multiplier
                channel_brand_safety_score.add_metadata_score(word.name, keyword_category, keyword_score)
            except KeyError:
                pass

        # If blacklist data available, then set blacklisted category score to 0
        for category_id in self.blacklist_data.keys():
            channel_brand_safety_score.category_scores[int(category_id)] = 0

        return channel_brand_safety_score

    def instantiate_es(self):
        """
        Instantiate Elasticsearch channel model with brand safety data
        :return:
        """
        brand_safety_score = getattr(self, constants.BRAND_SAFETY_SCORE)
        es_data = {
            "meta": {
                "id": self.metadata["id"],
            },
            "brand_safety": {
                "overall_score": brand_safety_score.overall_score if brand_safety_score.overall_score >= 0 else 0,
                "videos_scored": brand_safety_score.videos_scored,
                "language":  self.metadata["language"],
                "categories": {
                    category: {
                        "category_score": score,
                        "keywords": []
                    }
                    for category, score in brand_safety_score.category_scores.items()
                }
            }
        }
        for _, keyword_data in brand_safety_score.keyword_scores.items():
            # Pop category as we do not need to store in categories section, only needed for key access
            category = keyword_data.pop("category")
            es_data["brand_safety"]["categories"][category]["keywords"].append(keyword_data)
        channel = Channel(**es_data)
        return channel
