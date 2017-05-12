from django.core.cache import cache
from django.db.models import Count, Q

from utils.utils import move_us_to_top


class ChannelFiltersGenerator(object):
    def __init__(self, queryset):
        self.queryset = queryset

    def generate_countries_filter(self):
        countries = self.queryset.filter(country__isnull=False)\
                                 .values("country")\
                                 .annotate(channels_count=Count("id"))\
                                 .order_by("country")
        countries = list(countries)
        countries = move_us_to_top(countries)
        countries_filter = [{
            "country": obj.get("country"),
            "description": "{} {}".format(obj.get("channels_count"), "channels")
        } for obj in countries]
        return countries_filter

    def generate_categories_filter(self):
        categories = self.queryset.filter(category__isnull=False)\
                                  .values_list("category", flat=True)
        categories = {category for channel_category in categories for category in channel_category.split(",")}
        categories = list(categories)
        categories.sort()
        categories_filter = [{
            "category": obj,
            "description": "{} {}".format(self.queryset.filter(category__icontains=obj).count(), "channels")
        } for obj in categories]
        return categories_filter


class VideoFiltersGenerator(object):
    def __init__(self, queryset):
        self.queryset = queryset

    def set_cache(self, video_filter):
        video_filters = cache.get("video_filters")
        if not video_filters:
            video_filters = {}
        video_filters.update(video_filter)
        cache.set("video_filters", video_filters, timeout=None)

    def generate_channels_filter(self):
        channels = self.queryset.values("channel__title", "channel__id", "channel__details__videos")\
                                .order_by("-channel__details__videos")
        channels = channels.distinct()
        channels = [{
            'title': x["channel__title"],
            'id': x["channel__id"],
            "description": "{} {}".format(x["channel__details__videos"], "videos")
        } for x in channels if x['channel__id']]
        self.set_cache({"channels": channels})
        return channels

    def generate_statuses_filter(self):
        statuses = self.queryset.filter(status_id__isnull=False)\
                                .values("status__name", "status_id")\
                                .annotate(videos_count=Count("id"))\
                                .order_by("status_id")
        status_filter = [{
            "status": obj.get("status__name"),
            "description": "{} {}".format(obj.get("videos_count"), "videos"),
            "id": obj.get("status_id")
        } for obj in statuses]
        sub_statuses = self.queryset.filter(sub_status_id__isnull=False)\
                                    .values("sub_status__name", "sub_status_id", "status_id", "status__name")\
                                    .annotate(videos_count=Count("id"))\
                                    .order_by("sub_status_id")
        sub_status_filter = [{
            "status": "{} {}".format(obj.get("status__name"), obj.get("sub_status__name")),
            "description": "{} {}".format(obj.get("videos_count"), "videos"),
            "id": "{}_{}".format(obj.get("status_id"), obj.get("sub_status_id"))
        } for obj in sub_statuses]
        not_set_filter = [{
            "status": "Not set",
            "id": "null",
            "description": "{} {}".format(self.queryset.filter(status_id__isnull=True).count(), "videos")
        }]
        statuses = status_filter + sub_status_filter
        statuses = sorted(statuses, key=lambda k: k['status'])
        statuses = not_set_filter + statuses
        self.set_cache({"statuses": statuses})
        return statuses

    def generate_categories_filter(self):
        categories = self.queryset.filter(category__isnull=False)\
                                  .values("category")\
                                  .annotate(videos_count=Count("id"))\
                                  .order_by("category")
        categories_filter = [{
            "category": obj.get("category"),
            "description": "{} {}".format(obj.get("videos_count"), "videos")
        } for obj in categories]
        self.set_cache({"categories": categories_filter})
        return categories_filter

    def generate_countries_filter(self):
        countries = self.queryset.filter(country__isnull=False)\
                                 .values("country")\
                                 .annotate(videos_count=Count("id"))\
                                 .order_by("country")
        countries = list(countries)
        countries = move_us_to_top(countries)
        countries_filter = [{
            "country": obj.get("country"),
            "description": "{} {}".format(obj.get("videos_count"), "videos")
        } for obj in countries]
        self.set_cache({"countries": countries_filter})
        return countries_filter

    def generate_languages_filter(self):
        languages = self.queryset.filter(lang_code__isnull=False)\
                                 .values("lang_code")\
                                 .annotate(videos_count=Count("id"))\
                                 .order_by("lang_code")
        languages = [{
            "id": obj.get("lang_code"),
            "title": LANGUAGES.get(obj.get("lang_code")),
            "description": "{} {}".format(obj.get("videos_count"), "videos")
        } for obj in languages]
        self.set_cache({"languages": languages})
        return languages

    def generate_content_owners_filter(self):
        total_count = self.queryset.count()

        no_owner_count = self.queryset\
                             .filter(ptk_value="youtube_none")\
                             .count()

        channel_owner_count = self.queryset\
                                  .filter(ptk_value__contains="(CC)")\
                                  .count()

        cms_owner_count = self.queryset\
                              .filter(ptk_value__endswith="+user")\
                              .count()

        multiple_claimers_count = self.queryset\
                                      .filter(ptk_value="youtube_multi")\
                                      .count()

        unknown_count = self.queryset\
                            .filter(Q(ptk_value='(Not tagged)') | Q(ptk_value__isnull=True))\
                            .count()

        network_owner_count = total_count\
                            - no_owner_count\
                            - channel_owner_count\
                            - cms_owner_count\
                            - multiple_claimers_count\
                            - unknown_count

        filter_options = {
            "No Owner": no_owner_count,
            "Channel Owner": channel_owner_count,
            "CMS Owner": cms_owner_count,
            "Multiple Claimers": multiple_claimers_count,
            "Unknown": unknown_count,
            "Network Owner": network_owner_count,
        }
        content_owner_filter = [{
            "content_owner": key,
            "description": "{} {}".format(value, "videos")
        } for key, value in filter_options.items()]
        self.set_cache({"content_owners": content_owner_filter})
        return content_owner_filter

    def generate_all_filters(self):
        self.generate_segments_filter()
        self.generate_categories_filter()
        self.generate_countries_filter()
        self.generate_channels_filter()
        self.generate_statuses_filter()
        self.generate_content_owners_filter()
        video_filters = cache.get("video_filters")
        return video_filters
