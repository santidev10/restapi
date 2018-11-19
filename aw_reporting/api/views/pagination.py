from utils.api_paginator import CustomPageNumberPaginator


class PricingToolCampaignsPagination(CustomPageNumberPaginator):
    page_size = 10


class PacingReportOpportunitiesPaginator(CustomPageNumberPaginator):
    page_size = 10
    page_size_query_param = "size"
