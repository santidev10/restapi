from brand_safety import constants
from brand_safety.audit_models.brand_safety_video_score import BrandSafetyVideoScore
from brand_safety.models import BadWordCategory
from es_components.models import Video


class BrandSafetyVideoAudit(object):
    metadata = {}
    score_multiplier = {
        "title": 4,
        "description": 1,
        "tags": 1,
        "transcript": 1
    }

    def __init__(self, data, audit_utils, ignore_blacklist_data=False):
        self.audit_utils = audit_utils
        self.score_mapping = audit_utils.score_mapping
        self.default_category_scores = audit_utils.default_full_score
        self.language_processors = audit_utils.bad_word_processors_by_language
        self.ignore_blacklist_data = ignore_blacklist_data
        self._set_metadata(data)
        self.is_vetted = data.get("is_vetted")

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
        ])
        transcript_text = data.get("transcript", "") or ""
        detected = {
            "has_emoji": self.audit_utils.has_emoji(text + ", " + transcript_text)
        }
        if not data.get("language"):
            detected["language"] = self.audit_utils.get_language(text)
        if transcript_text and not data.get("transcript_language"):
            detected["transcript_language"] = self.audit_utils.get_language(transcript_text)
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
            universal_processor = self.language_processors['un']
        except KeyError:
            # Set the language the audit uses
            self.metadata["language"] = "all"
            keyword_processor = self.language_processors["all"]
            universal_processor = False
        try:
            transcript_processor = self.language_processors[self.metadata["transcript_language"]]
        except KeyError:
            transcript_processor = keyword_processor
        tag_hits = self.audit_utils.audit(self.metadata.get("tags", ""), constants.TAGS, keyword_processor)
        title_hits = self.audit_utils.audit(self.metadata.get("title", ""), constants.TITLE, keyword_processor)
        description_hits = self.audit_utils.audit(self.metadata.get("description", ""), constants.DESCRIPTION,
                                                  keyword_processor)
        transcript_hits = self.audit_utils.audit(self.metadata.get("transcript", ""), constants.TRANSCRIPT,
                                                 transcript_processor)
        all_hits = tag_hits + title_hits + description_hits + transcript_hits
        # Calculate Universal keywords hits, if not all processor
        if universal_processor:
            universal_tag_hits = self.audit_utils.audit(self.metadata.get("tags", ""), constants.TAGS,
                                                        universal_processor)
            universal_title_hits = self.audit_utils.audit(self.metadata.get("title", ""), constants.TITLE,
                                                          universal_processor)
            universal_description_hits = self.audit_utils.audit(self.metadata.get("description", ""),
                                                                constants.DESCRIPTION,
                                                                universal_processor)
            universal_transcript_hits = self.audit_utils.audit(self.metadata.get("transcript", ""),
                                                               constants.TRANSCRIPT,
                                                               universal_processor)
            all_hits += universal_tag_hits + universal_title_hits + universal_description_hits + \
                        universal_transcript_hits

        score = self.calculate_brand_safety_score(*all_hits)
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
            except KeyError:
                continue
            else:
                brand_safety_score.add_keyword_score(word.name, keyword_category, calculated_score)

        # If blacklist data available, then set overall score and blacklisted category score to 0
        if self.ignore_blacklist_data is False:
            for category_id in self.metadata.get("brand_safety_blacklist", []):
                brand_safety_score.category_scores[category_id] = 0
                if category_id not in BadWordCategory.EXCLUDED:
                    brand_safety_score.overall_score = 0

        return brand_safety_score

    def instantiate_es(self):
        video = Video(self.metadata["id"])
        """
        Instantiate Elasticsearch video model with brand safety data
        """
        brand_safety_score = getattr(self, constants.BRAND_SAFETY_SCORE)
        brand_safety_data = {
            "overall_score": brand_safety_score.overall_score if brand_safety_score.overall_score >= 0 else 0,
            "transcript_language": self.metadata.get("transcript_language"),
            "categories": {
                category: {
                    "category_score": category_score,
                    "keywords": [],
                    "severity_counts": self.audit_utils.default_severity_counts
                }
                for category, category_score in brand_safety_score.category_scores.items()
            }
        }
        for word, keyword_data in brand_safety_score.keyword_scores.items():
            try:
                # Pop category as we do not need to store in categories section, only needed for key access
                category = keyword_data.pop("category")
                brand_safety_data["categories"][category]["keywords"].append(keyword_data)

                # Increment category severity hit counts
                severity = str(self.score_mapping[word]["score"])
                brand_safety_data["categories"][category]["severity_counts"][severity] += 1
            except KeyError:
                continue

        video.brand_safety = brand_safety_data
        video.populate_channel(id=self.metadata["channel_id"], title=self.metadata["channel_title"])
        return video

    @staticmethod
    def instantiate_blocklist(item_id):
        blocklist_item = Video(item_id)
        blocklist_item.populate_brand_safety(overall_score=0)
        return blocklist_item
