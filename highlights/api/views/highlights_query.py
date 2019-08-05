class HighlightsQuery:
    allowed_filters = ("category__terms", "language__terms")
    allowed_sorts = ("thirty_days_subscribers", "thirty_days_views", "thirty_days_comments", "thirty_days_likes",
                     "weekly_subscribers", "weekly_views", "weekly_comments", "weekly_likes",
                     "daily_subscribers", "daily_views", "daily_comments", "daily_likes")
    allowed_sorts_type = ("desc",)
    allowed_aggregations = ("category", "language")

    default_size = "20"
    default_max_page = 5

    allowed_sizes = ("100", "20")
    allowed_max_pages = {
        "100": 1,
        "20": default_max_page,
    }

    def __init__(self, query_params):
        self.result_query_params = CustomQueryParamsDict()
        self.result_query_params._mutable = True
        self.result_query_params["updated_at__days_range"] = 3
        self.result_query_params["engage_rate__range"] = "1,"
        self.request_query_params = query_params

    def prepare_query(self, mode=None):
        fields = self.request_query_params.get("fields")
        if fields:
            self.result_query_params["fields"] = fields
        size = self.request_query_params.get("size")
        max_page = self.allowed_max_pages.get(size) or self.default_max_page
        if size not in self.allowed_sizes:
            size = self.default_size
        page = self.request_query_params.get("page")
        if page and int(page) <= max_page:
            self.result_query_params["page"] = page
        self.result_query_params["size"] = size
        self.result_query_params["max_page"] = max_page
        if self.request_query_params.get("sort"):
            sort, sort_type = self.request_query_params.get("sort").split(":", 1)
            if sort in self.allowed_sorts \
                    and sort_type in self.allowed_sorts_type:
                self.result_query_params["sort"] = self.request_query_params.get("sort")
        for allowed_filter in self.allowed_filters:
            if self.request_query_params.get(allowed_filter):
                filter_cat = self.request_query_params.get(allowed_filter)
                self.result_query_params[allowed_filter] = filter_cat.split(",")[0]
        aggregations = self.request_query_params.get("aggregations", "").split(",")
        if set(aggregations).issubset(set(self.allowed_aggregations)):
            self.result_query_params["aggregations"] = ",".join(aggregations)
        if mode is not None:
            pass
            # Disabled due to SAAS-1932
            # if mode == "video" or mode == "channel":
            #     self.result_query_params["language__terms"] = "English"
        return self.result_query_params

    @staticmethod
    def adapt_language_aggregation(response_data):
        language_aggregation = response_data.get("aggregations", {}).get("language:count", [])
        if language_aggregation:
            response_data["aggregations"]["language:count"] = language_aggregation[:10]
        return response_data


class CustomQueryParamsDict(dict):
    pass