class BrandSafetyChannelScore(object):
    """
        Class to encapsulate brand safety score logic for Channel brand safety audits
        """
    def __init__(self, video_id, num_videos, default_category_scores):
        self.video_id = video_id
        self.videos_scored = num_videos
        self.overall_score = 0
        self.keyword_scores = {}
        self.category_scores = default_category_scores
        self.average_calculated = False

    @property
    def pk(self):
        return self.video_id

    def add_keyword_score(self, keyword, category, negative_score, hits):
        """
        Add keyword score for each category
        :param keyword: str
        :param category: str
        :param negative_score: int: Keyword negative score derived from brand safety keyword score mapping
        :param hits: int
        :return: None
        """
        self.keyword_scores[keyword] = self.keyword_scores.get(keyword, {
            "category": category,
            "keyword": keyword,
            "hits": 0,
            "negative_score": 0,
        })
        self.keyword_scores[keyword]["hits"] += hits
        self.keyword_scores[keyword]["negative_score"] += negative_score

    def add_category_score(self, category, score):
        """
        Add category score from each Video
        :param category: str
        :param score: int
        :return:
        """
        self.category_scores[category] += score

    def add_overall_score(self, score):
        """
        Add score to Channel overall score for each Video audit
        :param score:
        :return:
        """
        self.overall_score += score

    def calculate_average_scores(self):
        """
        Average aggregated Video scores for Channel
            Overall score is simple average of sum of Video scores
            Category score is simple average of number of Video's with scores within the category
        :param num_videos: Number of video Channel owns
        :return: None
        """
        self.overall_score //= self.videos_scored
        for category in self.category_scores.keys():
            self.category_scores[category] //= self.videos_scored
            # self.category_scores[category]["category_score"] = self.category_scores[category]["category_score"] // self.category_scores[category]["count"]
        self.average_calculated = True

    def add_metadata_score(self, keyword, category, score):
        """
        Add Channel metadata scores
            This method must be called after calculcate_average_scores as channel metadata scores must be calculcated with
            the channel's average scores
        :param keyword: str
        :param category: str
        :param score: int
        :return: None
        """
        if not self.average_calculated:
            raise BrandSafetyChannelScoreException("You must call calculate_average_scores before calling add_metadata_score.")
        self.overall_score -= score
        self.add_keyword_score(keyword, category, score, 1)


class BrandSafetyChannelScoreException(Exception):
    pass
