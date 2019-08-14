from brand_safety import constants
from brand_safety.audit_models.brand_safety_video_score import BrandSafetyVideoScore
from es_components.models import Video


class BrandSafetyVideoAudit(object):
    metadata = {}
    score_multiplier = {
        "title": 4,
        "description": 1,
        "tags": 1,
        "transcript": 1
    }

    def __init__(self, data, audit_utils):
        self.audit_utils = audit_utils
        self.score_mapping = audit_utils.score_mapping
        self.default_category_scores = audit_utils.default_video_score
        self.language_processors = audit_utils.bad_word_processors_by_language
        self._set_metadata(data)

    @property
    def pk(self):
        pk = self.metadata["id"]
        return pk

    def _set_metadata(self, data: dict) -> None:
        """
        Set metadata with language and emoji
        :param data -> keys = channel_id, description, channel_title
        :return:
        """
        text = ", ".join([
            data.get("title", "") or "",
            data.get("description", "") or "",
            data.get("tags", "") or "",
            data.get("transcript", "") or "",
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
        # Try to get video language processor
        try:
            keyword_processor = self.language_processors[self.metadata["language"]]
        except KeyError:
            # Set the language the audit uses
            self.metadata["language"] = "all"
            keyword_processor = self.language_processors["all"]
        tag_hits = self.audit_utils.audit(self.metadata["tags"], constants.TAGS, keyword_processor)
        title_hits = self.audit_utils.audit(self.metadata["title"], constants.TITLE, keyword_processor)
        description_hits = self.audit_utils.audit(self.metadata["description"], constants.DESCRIPTION, keyword_processor)
        transcript_hits = self.audit_utils.audit(self.metadata["transcript"], constants.TRANSCRIPT, keyword_processor)

        score = self.calculate_brand_safety_score(*tag_hits + title_hits + description_hits + transcript_hits)
        setattr(self, constants.BRAND_SAFETY_SCORE, score)

    def calculate_brand_safety_score(self, *hits):
        """
        Calculate brand safety score total and across categories
        :return: tuple -> (int) total score, (dict) scores by category
        """
        brand_safety_score = BrandSafetyVideoScore(self.pk, self.default_category_scores)
        for word in hits:
            multiplier = self.score_multiplier.get(word.location, 1)
            try:
                keyword_category = self.score_mapping[word.name]["category"]
                keyword_score = self.score_mapping[word.name]["score"]
                calculated_score = keyword_score * multiplier
                brand_safety_score.add_keyword_score(word.name, keyword_category, calculated_score)
            except KeyError:
                pass
        return brand_safety_score

    def instantiate_es(self):
        """
        Instantiate Elasticsearch video model with brand safety data
        :return:
        """
        brand_safety_score = getattr(self, constants.BRAND_SAFETY_SCORE)
        es_data = {
            "meta": {
                "id": self.metadata["id"],
            },
            "brand_safety": {
                "overall_score": brand_safety_score.overall_score if brand_safety_score.overall_score >= 0 else 0,
                "language": self.metadata["language"],
                "categories": {
                    category: {
                        "category_score": category_score,
                        "keywords": []
                    }
                    for category, category_score in brand_safety_score.category_scores.items()
                }

            }
        }
        for _, keyword_data in brand_safety_score.keyword_scores.items():
            # Pop category as we do not need to store in categories section, only needed for key access
            category = keyword_data.pop("category")
            es_data["brand_safety"]["categories"][category]["keywords"].append(keyword_data)
        video = Video(**es_data)
        return video
