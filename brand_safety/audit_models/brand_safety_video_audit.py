from brand_safety import constants
from brand_safety.audit_models.brand_safety_video_score import BrandSafetyVideoScore
from brand_safety.auditors.utils import AuditUtils
from es_components.models import Video


class BrandSafetyVideoAudit(object):
    """
    Brand safety audit for channels
    Gathers text metadata and applies brand safety scoring by detecting channel language, detecting words,
    calculating scores relative to the word, hit location (e.g. title, description)
    """
    # This is used to add weight to a negative score depending on where a word was detected. E.g. A video having
    # a bad word is 4 times worse than in its tags
    score_multiplier = {
        "title": 4,
        "description": 1,
        "tags": 1,
        "transcript": 1
    }

    def __init__(self, video: Video, audit_utils: AuditUtils):
        self.audit_utils = audit_utils
        self.score_mapping = audit_utils.score_mapping
        self.default_category_scores = audit_utils.default_full_score
        self.language_processors = audit_utils.bad_word_processors_by_language
        self.audit_metadata = self._set_metadata(video)
        self.doc = video

    def _set_metadata(self, video):
        """
        Set audit metadata with language
        This detects the language of the Video using all text metadata
        :param video -> es_components Video obj
        :return:
        """
        text = ", ".join([
            video.general_data.title or "",
            video.general_data.description or "",
            video.tags or "",
        ])
        transcript_text = video.transcript or ""
        audit_metadata = {}
        if not video.general_data.lang_code:
            audit_metadata["language"] = self.audit_utils.get_language(text)
        else:
            audit_metadata["language"] = video.general_data.lang_code

        # if transcript_text and not data.get("transcript_language"):
        if transcript_text and not video.transcript_language:
            audit_metadata["transcript_language"] = self.audit_utils.get_language(transcript_text)
        return audit_metadata

    def run(self):
        """
        Call all required audit methods here
        :return:
        """
        self._run_brand_safety_audit()
        return self

    def _run_brand_safety_audit(self):
        """
        Search for keyword hits using keyword processors depending on the detected language of the Video
        If valid language is detected, keyword processor uses language and universal language processor to
        detect words. The use of universal language is required to obtain word hits that span across all languages
        If no valid language detected, then defaults to "all" which also includes universal language processor
        :return: None
        """
        # Try to get video language processor
        try:
            keyword_processor = self.language_processors[self.audit_metadata.get("language")]
            universal_processor = self.language_processors["un"]
        except KeyError:
            # Set the language the audit uses
            self.audit_metadata["language"] = "all"
            keyword_processor = self.language_processors["all"]
            universal_processor = False
        try:
            transcript_processor = self.language_processors[self.audit_metadata["transcript_language"]]
        except KeyError:
            transcript_processor = keyword_processor

        # Detect hits in each metadata section, saving hit location and location weight
        tag_hits = self.audit_utils.audit(self.doc.tags, constants.TAGS, keyword_processor)
        title_hits = self.audit_utils.audit(self.doc.general_data.title, constants.TITLE, keyword_processor)
        description_hits = self.audit_utils.audit(self.doc.general_data.description, constants.DESCRIPTION, keyword_processor)
        transcript_hits = self.audit_utils.audit(self.doc.transcript, constants.TRANSCRIPT, transcript_processor)
        all_hits = tag_hits + title_hits + description_hits + transcript_hits

        # Calculate Universal keywords hits, if not all processor
        if universal_processor:
            universal_tag_hits = self.audit_utils.audit(self.doc.tags, constants.TAGS,
                                                        universal_processor)
            universal_title_hits = self.audit_utils.audit(self.doc.general_data.title, constants.TITLE,
                                                          universal_processor)
            universal_description_hits = self.audit_utils.audit(self.doc.general_data.description,
                                                                constants.DESCRIPTION,
                                                                universal_processor)
            universal_transcript_hits = self.audit_utils.audit(self.doc.transcript,
                                                               constants.TRANSCRIPT,
                                                               universal_processor)
            all_hits += universal_tag_hits + universal_title_hits + universal_description_hits + \
                        universal_transcript_hits

        score = self.calculate_brand_safety_score(*all_hits)
        setattr(self, constants.BRAND_SAFETY_SCORE, score)

    def calculate_brand_safety_score(self, *hits) -> BrandSafetyVideoScore:
        """
        Calculate brand safety score total and across categories
        This uses multipliers which depends on the location the word was found to determine a negative score
        to apply to both the overall score and category score
        :return: tuple -> (int) total score, (dict) scores by category
        """
        brand_safety_score = BrandSafetyVideoScore(self.doc.main.id, self.default_category_scores)
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

        if self.doc.custom_properties.blocklist is True:
            brand_safety_score.overall_score = 0

        return brand_safety_score

    def add_brand_safety_data(self, reset_rescore=False) -> Video:
        """
        Add brand safety data to ES video document
        :param reset_rescore: bool -> If True, set rescore=False
        """
        video = self.doc
        brand_safety_score = getattr(self, constants.BRAND_SAFETY_SCORE)
        brand_safety_data = {
            "created_at": video.brand_safety.created_at,
            "updated_at": video.brand_safety.updated_at,
            "overall_score": brand_safety_score.overall_score if brand_safety_score.overall_score >= 0 else 0,
            "transcript_language": self.doc.transcript_language,
            "categories": {
                # Prepare empty categories for filling
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
                # Exclude category as we do not need to store in categories section, only needed for key access
                category = keyword_data["category"]
                data = {key: val for key, val in keyword_data.items() if key != "category"}
                brand_safety_data["categories"][category]["keywords"].append(data)

                # Increment category severity hit counts
                severity = str(self.score_mapping[word]["score"])
                brand_safety_data["categories"][category]["severity_counts"][severity] += 1
            except KeyError:
                continue
        if reset_rescore is True:
            brand_safety_data["rescore"] = False
        video.brand_safety = brand_safety_data
        return video
