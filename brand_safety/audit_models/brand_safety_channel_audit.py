from brand_safety import constants
from brand_safety.audit_models.brand_safety_channel_score import BrandSafetyChannelScore
from brand_safety.models import BadWordCategory
from es_components.models import Channel


class BrandSafetyChannelAudit(object):
    """
    Brand safety audit for channels
    Gathers text metadata and applies brand safety scoring by detecting channel language, detecting words,
    calculating scores relative to the word, hit location (e.g. title, description)
    """
    score_multiplier = {
        "title": 4,
        "description": 1,
        "tags": 1,
        "transcript": 1
    }

    def __init__(self, channel, audit_utils):
        self.video_audits = channel.video_audits
        self.doc = channel
        self.audit_utils = audit_utils
        self.score_mapping = audit_utils.score_mapping
        # If channel has video audits, then channel should start with default_zero_score to find average category scores
        # Else, channel should start with full_score since no average can be calculated and will be used to subtract
        # metadata scores
        self.default_category_scores = audit_utils.default_zero_score if len(
            self.video_audits) > 0 else audit_utils.default_full_score
        self.language_processors = audit_utils.bad_word_processors_by_language
        self.audit_metadata = self._get_metadata(channel)

    def _get_metadata(self, channel: Channel) -> dict:
        """
        Get audit metadata to be used during scoring
        :param channel: Channel
        :return:
        """
        text_mapping = {
            "title": channel.general_data.title or "",
            "description": channel.general_data.description or "",
            "video_tags": channel.video_tags or "",
        }
        text = ",".join(text_mapping.keys())
        audit_data = {
            "language": self.audit_utils.get_language(text),
            **text_mapping
        }
        return audit_data

    def run(self):
        """
        Call all required audit methods here
        :return:
        """
        self._run_brand_safety_audit()

    def _run_brand_safety_audit(self):
        """
        Search for keyword hits using keyword processors depending on the detected language of the Channel
        If valid language is detected, keyword processor uses language and universal language processor to
        detect words. The use of universal language is required to obtain word hits that span across all languages
        If no valid language detected, then defaults to "all" which also includes universal language processor
        :return:
        """
        # Try to get channel language processor
        try:
            keyword_processor = self.language_processors[self.audit_metadata["language"]]
            universal_processor = self.language_processors["un"]
        except KeyError:
            # Set the language the audit uses
            self.audit_metadata["language"] = "all"
            keyword_processor = self.language_processors["all"]
            universal_processor = False
        title_hits = self.audit_utils.audit(self.audit_metadata["title"], constants.TITLE, keyword_processor)
        description_hits = self.audit_utils.audit(self.audit_metadata["description"], constants.DESCRIPTION,
                                                  keyword_processor)
        all_hits = title_hits + description_hits
        # Universal keywords hits
        if universal_processor:
            universal_title_hits = self.audit_utils.audit(self.audit_metadata["title"], constants.TITLE, universal_processor)
            universal_description_hits = self.audit_utils.audit(self.audit_metadata["description"], constants.DESCRIPTION,
                                                                universal_processor)
            all_hits += universal_title_hits + universal_description_hits

        score = self.calculate_brand_safety_score(*all_hits)
        setattr(self, constants.BRAND_SAFETY_SCORE, score)

    def calculate_brand_safety_score(self, *channel_metadata_hits, **_):
        """
        Aggregate video audit brand safety results by averaging all video keyword hits and combining with channel
        metadata scores
        :param channel_metadata_hits: All channel metadata hits
        :return:
        """
        channel_brand_safety_score = BrandSafetyChannelScore(self.doc.main.id, len(self.video_audits),
                                                             self.default_category_scores)
        # Aggregate video audits scores for channel
        for audit in self.video_audits:
            video_brand_safety_score = getattr(audit, constants.BRAND_SAFETY_SCORE)
            for keyword_name, data in video_brand_safety_score.keyword_scores.items():
                channel_brand_safety_score.add_keyword_score(keyword_name, data["category"], data["negative_score"],
                                                             data["hits"])

            for category, score in video_brand_safety_score.category_scores.items():
                if category in channel_brand_safety_score.category_scores:
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

                if keyword_category is None:
                    continue
            except KeyError:
                continue
            else:
                channel_brand_safety_score.add_metadata_score(word.name, keyword_category, keyword_score)

        if self.doc.custom_properties.blocklist is True:
            channel_brand_safety_score.overall_score = 0

        return channel_brand_safety_score

    def instantiate_es(self):
        """
        Instantiate Elasticsearch channel model with brand safety data
        """
        channel = Channel(self.doc.main.id)
        brand_safety_score = getattr(self, constants.BRAND_SAFETY_SCORE)
        brand_safety_data = {
            "overall_score": brand_safety_score.overall_score if brand_safety_score.overall_score >= 0 else 0,
            "videos_scored": brand_safety_score.videos_scored,
            "language": self.audit_metadata["language"],
            "categories": {
                category: {
                    "category_score": score,
                    "keywords": [],
                    "severity_counts": self.audit_utils.default_severity_counts
                }
                for category, score in brand_safety_score.category_scores.items()
            }
        }
        for word, keyword_data in brand_safety_score.keyword_scores.items():
            try:
                # Pop category as we do not need to store in categories section, only needed for key access
                category = keyword_data.get("category")
                data = {key: val for key, val in keyword_data.items() if key != "category"}
                if category is None:
                    category = self.score_mapping[word]["category"]
                brand_safety_data["categories"][category]["keywords"].append(data)

                # Increment category severity hit counts
                severity = str(self.score_mapping[word]["score"])
                brand_safety_data["categories"][category]["severity_counts"][severity] += 1
            except KeyError:
                continue
        channel.brand_safety = brand_safety_data
        return channel
