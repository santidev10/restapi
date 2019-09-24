TERMS_FILTER = ("main.id", "stats.is_viral", "stats.top_category",)
MATCH_PHRASE_FILTER = ("main.id",)

RANGE_FILTER = ("stats.search_volume", "stats.average_cpc", "stats.competition",)

KEYWORD_CSV_HEADERS = [
    "keyword",
    "search_volume",
    "average_cpc",
    "competition",
    "video_count",
    "views",
]
