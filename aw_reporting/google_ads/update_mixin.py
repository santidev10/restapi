from aw_reporting.google_ads.constants import MIN_FETCH_DATE
from aw_reporting.google_ads.cta import get_clicks_report
from aw_reporting.google_ads.cta import get_stats_with_click_type_data
from aw_reporting.google_ads.utils import drop_latest_stats
from aw_reporting.google_ads.utils import drop_custom_stats
from aw_reporting.google_ads.utils import extract_placement_code
from aw_reporting.google_ads.utils import format_query
from aw_reporting.google_ads.utils import get_account_border_dates
from aw_reporting.google_ads.utils import get_base_stats
from aw_reporting.google_ads.utils import get_date_range
from aw_reporting.google_ads.utils import get_geo_target_constants
from aw_reporting.google_ads.utils import get_quartile_views
from aw_reporting.google_ads.utils import max_ready_date
from aw_reporting.google_ads.utils import reset_denorm_flag


class UpdateMixin(object):
    MIN_FETCH_DATE = MIN_FETCH_DATE

    @staticmethod
    def get_clicks_report(*args, **kwargs):
        return get_clicks_report(*args, **kwargs)

    @staticmethod
    def get_stats_with_click_type_data(*args, **kwargs):
        return get_stats_with_click_type_data(*args, **kwargs)

    @staticmethod
    def get_quartile_views(*args, **kwargs):
        return get_quartile_views(*args, **kwargs)

    @staticmethod
    def format_query(*args, **kwargs):
        return format_query(*args, **kwargs)

    @staticmethod
    def get_date_range(*args, **kwargs):
        return get_date_range(*args, **kwargs)

    @staticmethod
    def get_base_stats(*args, **kwargs):
        return get_base_stats(*args, **kwargs)

    @staticmethod
    def drop_latest_stats(*args, **kwargs):
        return drop_latest_stats(*args, **kwargs)

    @staticmethod
    def get_account_border_dates(*args, **kwargs):
        return get_account_border_dates(*args, **kwargs)

    @staticmethod
    def extract_placement_code(*args, **kwargs):
        return extract_placement_code(*args, **kwargs)

    @staticmethod
    def get_geo_target_constants(*args, **kwargs):
        return get_geo_target_constants(*args, **kwargs)

    @staticmethod
    def max_ready_date(*args, **kwargs):
        return max_ready_date(*args, **kwargs)

    @staticmethod
    def drop_custom_stats(*args, **kwargs):
        return drop_custom_stats(*args, **kwargs)

    @staticmethod
    def reset_denorm_flag(*args, **kwargs):
        return reset_denorm_flag(*args, **kwargs)
