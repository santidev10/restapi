from cache.utils import RequestParamsMock
from dashboard.api.views import DashboardIndustryPerformanceAPIView
from saas import celery_app
from saas.configs.celery import TaskExpiration
from saas.configs.celery import TaskTimeout


@celery_app.task(expires=TaskExpiration.INDUSTRY_PERFORMANCE_CACHING,
                 soft_time_limit=TaskTimeout.INDUSTRY_PERFORMANCE_CACHING)
def cache_industry_performance():
    industry_performance_api = DashboardIndustryPerformanceAPIView()
    shared_sorts = industry_performance_api.ALLOWED_VIDEO_SORTS
    query_param_sets = [
        {
            "channel_sort": shared_sorts[i],
            "video_sort": shared_sorts[i],
            "category_sort": shared_sorts[i],
        } for i in range(len(shared_sorts))
    ]
    query_param_sets.append(
        {
            "channel_sort": "stats.last_30day_subscribers",
            "category_sort": "stats.last_30day_subscribers"
        }
    )
    for query_params in query_param_sets:
        request = RequestParamsMock(query_params)
        industry_performance_api.get(request=request)
