class BrandSafetyVideoScore(object):
    """
    Class to encapsulate brand safety score state for Video brand safety audits
    """
    def __init__(self, video_id, default_category_scores):
        self.video_id = video_id
        self.overall_score = 100
        self.keyword_scores = {}
        self.category_scores = default_category_scores

    @property
    def pk(self):
        return self.video_id

    def add_keyword_score(self, keyword, category, negative_score):
        """
        Add keyword score for each category in each Video
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
        self.keyword_scores[keyword]["hits"] += 1
        self.keyword_scores[keyword]["negative_score"] += negative_score
        self.category_scores[category] -= negative_score
        self.overall_score -= negative_score
