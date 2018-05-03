from utils.api_paginator import CustomPageNumberPaginator


class AccountsListPaginator(CustomPageNumberPaginator):
    page_size = 20


class PricingToolCampaignsPagination(CustomPageNumberPaginator):
    page_size = 10


class PacingReportOpportunitiesPaginator(CustomPageNumberPaginator):
    page_size = 10
