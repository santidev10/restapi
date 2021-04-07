from elasticsearch_dsl import Q
from elasticsearch_dsl.query import Bool
from typing import Tuple

from audit_tool.constants import CHOICE_UNKNOWN_KEY
from audit_tool.models import AuditAgeGroup
from audit_tool.models import AuditCategory
from audit_tool.models import AuditContentQuality
from audit_tool.models import AuditContentType
from audit_tool.models import AuditGender
from es_components.constants import Sections
from es_components.countries import COUNTRY_CODES
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import VIDEO_CHANNEL_ID_FIELD
from es_components.managers import ChannelManager
from es_components.managers import VideoManager
from es_components.query_builder import QueryBuilder
from es_components.query_repository import get_last_vetted_at_exists_filter
from segment.models.constants import SegmentTypeEnum
from segment.models.constants import SegmentVettingStatusEnum
from utils.brand_safety import map_score_threshold


# pylint: disable=too-many-instance-attributes
class SegmentQueryBuilder:
    MAX_RETRIES = 1000
    MAX_SIZE = 10000
    SECTIONS = (Sections.MAIN, Sections.GENERAL_DATA, Sections.STATS, Sections.BRAND_SAFETY, Sections.ADS_STATS,
                Sections.TASK_US_DATA)

    AD_STATS_RANGE_FIELDS = ("video_view_rate", "average_cpv", "average_cpm", "ctr", "ctr_v", "video_quartile_100_rate")
    STATS_RANGE_FIELDS = ("last_30day_views",)

    def __init__(self, data, with_forced_filters=True, exclude_blocklist=True):
        """
        :param data: dict -> Query options
        :param video_ids: str -> Youtube ID (Query videos with channel_id=related_to)
        """
        self.with_forced_filters = with_forced_filters
        self._exclude_blocklist = exclude_blocklist
        # When returning _get_query_params, ui expects the score threshold that was originally passed in
        self._original_score_threshold = data.get("score_threshold")
        self._params = self._map_params(data)

        self.es_manager = VideoManager(sections=self.SECTIONS) \
            if data.get("segment_type") in [SegmentTypeEnum.VIDEO.value, "video"] \
            else ChannelManager(sections=self.SECTIONS)
        self.query_body = self._construct_query()
        self.query_params = self._get_query_params()

    def _map_params(self, params):
        _params = params.copy()
        _params["sentiment"] = self._map_sentiment(_params.pop("sentiment", None))
        _params["score_threshold"] = self._map_score_threshold(_params.pop("score_threshold", None))
        return _params

    def _map_published_at(self, segment_type):
        """
        :return: dict
        """
        if segment_type == SegmentTypeEnum.VIDEO.value:
            published_at = "general_data.youtube_published_at"
        else:
            published_at = "stats.last_video_published_at"
        return published_at

    def execute(self, limit=5):
        results = self.es_manager.search(self.query_body, limit=limit).extra(track_total_hits=True).execute()
        return results

    def _get_query_params(self):
        """ Get params used to construct query """
        query_params = self._params.copy()
        query_params.update({
            "score_threshold": self._original_score_threshold
        })
        return query_params

    # pylint: disable=too-many-branches,too-many-statements
    def _construct_query(self):
        """
        Construct Elasticsearch query for segment items
        :param config: dict
        :return: dict
        """
        must_queries = []
        should_queries = []
        segment_type = self._params.get("segment_type")

        if self._params.get("minimum_views"):
            min_views_ct_queries = self.get_numeric_include_na_queries(
                attr_name="minimum_views",
                flag_name="minimum_views_include_na",
                field_name="stats.views"
            )
            must_queries.append(min_views_ct_queries)

        minimum_duration = self._params.get("minimum_duration", None)
        if segment_type == SegmentTypeEnum.VIDEO.value and minimum_duration:
            minimum_duration_query = QueryBuilder().build().must().range() \
                .field(f"{Sections.GENERAL_DATA}.duration").gte(minimum_duration).get()
            must_queries.append(minimum_duration_query)

        maximum_duration = self._params.get("maximum_duration", None)
        if segment_type == SegmentTypeEnum.VIDEO.value and maximum_duration:
            maximum_duration_query = QueryBuilder().build().must().range() \
                .field(f"{Sections.GENERAL_DATA}.duration").lte(maximum_duration).get()
            must_queries.append(maximum_duration_query)

        if segment_type == SegmentTypeEnum.CHANNEL.value and self._params.get("minimum_subscribers"):
            min_subs_ct_queries = self.get_numeric_include_na_queries(
                attr_name="minimum_subscribers",
                flag_name="minimum_subscribers_include_na",
                field_name="stats.subscribers"
            )
            must_queries.append(min_subs_ct_queries)

        if segment_type == SegmentTypeEnum.CHANNEL.value and self._params.get("minimum_videos"):
            min_vid_ct_queries = self.get_numeric_include_na_queries(
                attr_name="minimum_videos",
                flag_name="minimum_videos_include_na",
                field_name="stats.total_videos_count"
            )
            must_queries.append(min_vid_ct_queries)

        # handle all
        for options, valid_options, field_name in [
            [
                self._params.get("gender", []),
                AuditGender.to_str_with_unknown.keys(),
                f"{Sections.TASK_US_DATA}.gender"],
            [
                self._params.get("age_groups", []),
                AuditAgeGroup.to_str_with_unknown.keys(),
                f"{Sections.TASK_US_DATA}.age_group"],
            [
                self._params.get("content_type", []),
                AuditContentType.to_str_with_unknown.keys(),
                f"{Sections.TASK_US_DATA}.content_type"],
            [
                self._params.get("content_quality", []),
                AuditContentQuality.to_str_with_unknown.keys(),
                f"{Sections.TASK_US_DATA}.content_quality"],
        ]:
            choice_unknown_query = self.get_query_for_choice_unknown_field(
                options=options,
                valid_options=valid_options,
                field_name=field_name
            )
            if isinstance(choice_unknown_query, Bool):
                must_queries.append(choice_unknown_query)

        if self._params.get("video_ids"):
            must_queries.append(QueryBuilder().build().must().terms().field("main.id")
                                .value(self._params["video_ids"]).get())

        if self._params.get("last_upload_date"):
            published_at_field = self._map_published_at(segment_type)
            must_queries.append(QueryBuilder().build().must().range()
                                .field(published_at_field).gte(self._params["last_upload_date"]).get())

        if self._params.get("sentiment"):
            must_queries.append(
                QueryBuilder().build().must().range().field(f"{Sections.STATS}.sentiment")
                    .gte(self._params["sentiment"]).get()
            )

        if self._params.get("languages"):
            lang_code_field = "lang_code" if segment_type == SegmentTypeEnum.VIDEO.value else "top_lang_code"
            lang_queries = Q("bool")
            if self._params.get("languages_include_na"):
                lang_queries |= QueryBuilder().build().must_not().exists() \
                    .field(f"{Sections.GENERAL_DATA}.{lang_code_field}").get()
            for lang in self._params["languages"]:
                lang_queries |= QueryBuilder().build().should().term() \
                    .field(f"{Sections.GENERAL_DATA}.{lang_code_field}").value(lang).get()
            must_queries.append(lang_queries)

        if self._params.get("exclude_content_categories"):
            content_exclusion_queries = self._get_terms_query(self._params["exclude_content_categories"],
                                                              "general_data.iab_categories", "must_not")
            primary_category_exclusion = self._get_terms_query(self._params["exclude_content_categories"],
                                                               "general_data.primary_category", "must_not")
            must_queries.extend((content_exclusion_queries, primary_category_exclusion))

        if self._params.get("content_categories"):
            content_queries = Q("bool")
            content_categories = set(self._params["content_categories"]) \
                                 - set(self._params.get("exclude_content_categories", []))
            for category in content_categories:
                content_queries |= QueryBuilder().build().should().term().field("general_data.iab_categories").value(
                    category).get()
            must_queries.append(content_queries)

        if self._params.get("countries"):
            country_queries = Q("bool")
            if self._params.get("countries_include_na"):
                country_queries |= QueryBuilder().build().must_not().exists().field("general_data.country_code").get()
            for country in self._params["countries"]:
                country_code = COUNTRY_CODES.get(country)
                country_queries |= QueryBuilder().build().should().term().field("general_data.country_code").value(
                    country_code).get()
            must_queries.append(country_queries)

        if self._params.get("severity_filters"):
            severity_queries = Q("bool")
            for category, scores in self._params["severity_filters"].items():
                for score in scores:
                    # Querying for categories with at least one unique word of target severity score
                    severity_queries &= QueryBuilder().build().must().range().field(
                        f"brand_safety.categories.{category}.severity_counts.{score}").lte(0).get()
            must_queries.append(severity_queries)

        if self._params.get("score_threshold") is not None:
            score_threshold = self._params["score_threshold"]
            score_queries = Q("bool")
            if score_threshold == 0:
                score_queries |= QueryBuilder().build().must_not().exists().field("brand_safety.overall_score").get()
            score_queries |= QueryBuilder().build().must().range().field("brand_safety.overall_score").gte(
                score_threshold).get()
            must_queries.append(score_queries)

            if self._params.get("brand_safety_categories"):
                safety_queries = Q("bool")
                for category in self._params["brand_safety_categories"]:
                    safety_queries &= QueryBuilder().build().must().range().field(
                        f"brand_safety.categories.{category}.category_score").gte(score_threshold).get()
                must_queries.append(safety_queries)

        if self._params.get("is_vetted") is not None:
            vetted_query = QueryBuilder().build().must().exists().field("task_us_data.last_vetted_at").get() \
                if self._params["is_vetted"] \
                else QueryBuilder().build().must_not().exists().field("task_us_data.last_vetted_at").get()
            must_queries.append(vetted_query)

        if self._params.get("vetted_after"):
            vetted_after_query = QueryBuilder().build().must().range() \
                .field(f"{Sections.TASK_US_DATA}.last_vetted_at") \
                .gte(self._params["vetted_after"]).get()
            must_queries.append(vetted_after_query)

        if self._params.get("ias_verified_date"):
            ias_verified_query = QueryBuilder().build().must().range() \
                .field(f"{Sections.IAS_DATA}.ias_verified") \
                .gte(self._params["ias_verified_date"]).get()
            must_queries.append(ias_verified_query)

        if self._params.get("vetting_status") is not None and len(self._params.get("vetting_status", [])) > 0:
            vetting_status_queries = Q("bool")
            for status in self._params["vetting_status"]:
                if status == SegmentVettingStatusEnum.NOT_VETTED.value:
                    vetting_status_queries |= QueryBuilder().build().must_not().exists() \
                        .field(f"{Sections.TASK_US_DATA}.last_vetted_at").get()
                elif status == SegmentVettingStatusEnum.VETTED_SAFE.value:
                    vetting_status_safe = Q("bool")
                    vetting_status_safe &= QueryBuilder().build().must_not().exists() \
                        .field(f"{Sections.TASK_US_DATA}.brand_safety").get()
                    vetting_status_safe &= get_last_vetted_at_exists_filter()
                    vetting_status_queries |= vetting_status_safe
                elif status == SegmentVettingStatusEnum.VETTED_RISKY.value:
                    vetting_status_risky = Q("bool")
                    vetting_status_risky &= QueryBuilder().build().must().exists() \
                        .field(f"{Sections.TASK_US_DATA}.brand_safety").get()
                    vetting_status_risky &= get_last_vetted_at_exists_filter()
                    vetting_status_queries |= vetting_status_risky
            must_queries.append(vetting_status_queries)

        ads_stats_queries = self._get_ads_stats_queries()
        if self._params.get("ads_stats_include_na") is True:
            should_queries.append(QueryBuilder().build().must_not().exists().field(Sections.ADS_STATS).get())
        if self._params.get("last_30day_views"):
            query = self._get_range_queries(["last_30day_views"], Sections.STATS)
            if self._params.get("ads_stats_include_na") is True:
                query |= QueryBuilder().build().must_not().exists().field(f"{Sections.STATS}.last_30day_views").get()
            ads_stats_queries &= query
        must_queries.append(ads_stats_queries)
        
        query = Q("bool", must=must_queries)

        if self.with_forced_filters is True:
            forced_filters = self.es_manager.forced_filters()
            query &= forced_filters

        if self._exclude_blocklist is True:
            query &= QueryBuilder().build().must_not().term().field(f"{Sections.CUSTOM_PROPERTIES}.blocklist")\
                .value(True).get()

        if self._params.get("mismatched_language", None) is True:
            """ if mistmached_langauge is True, exclude all docs where mismatched_language=True """
            val = self._params["mismatched_language"]
            mismatched_language_query = QueryBuilder().build().must_not().term().field(
                "task_us_data.mismatched_language").value(val).get()
            query &= mismatched_language_query

        # Extend should queries last as combining queries with other queries (i.e. combining with forced_filters)
        # with operators (e.g. &, |) does not properly combine should queries
        query._params["should"].extend(should_queries)

        if segment_type == SegmentTypeEnum.VIDEO.value and self._params.get("minimum_subscribers"):
            query &= self._get_video_channels_subscribers_query(current_query=query)

        return query

    @staticmethod
    def get_query_for_choice_unknown_field(options: list, valid_options: list, field_name: str) -> Tuple[Bool, None]:
        """
        build a query for any field that uses a value of -1 (CHOICE_UNKNOWN_KEY) to include
        values where the field does not exist, like the include_na option
        :param options: the list of selected options
        :param valid_options: the full list of available options (used to check if we need a query at all)
        :param field_name: the name of the ES field to be queried
        :return: Bool or None
        """
        # if all options are selected, then we don't need to filter
        if not options or set(options) == set(valid_options):
            return None
        query = Q("bool")
        # -1 (CHOICE_UNKNOWN_KEY) is treated as include_na here
        if CHOICE_UNKNOWN_KEY in options:
            options.remove(CHOICE_UNKNOWN_KEY)
            query |= QueryBuilder().build().must_not().exists().field(field_name).get()
        for option in options:
            query |= QueryBuilder().build().should().term().field(field_name).value(option).get()
        return query

    # pylint: enable=too-many-branches,too-many-statements

    def get_numeric_include_na_queries(self, attr_name: str, flag_name: str, field_name: str):
        """
        get the combined queries for a gte field that supports the "include n/a" option
        :param attr_name: str, name of the attribute for this class
        :param flag_name: str, name of the include n/a flag attribute
        :param field_name: str, name of the dot-notated field in ES
        :return Q: the constructed Q query
        """
        queries = Q("bool")
        if self._params.get(flag_name):
            if flag_name == "minimum_subscribers_include_na":
                queries |= QueryBuilder().build().should().term().field("stats.hidden_subscriber_count").value(
                    True).get()
            else:
                queries |= QueryBuilder().build().should().term().field(field_name).value(0).get()
        queries |= QueryBuilder().build().should().range().field(field_name) \
            .gte(self._params.get(attr_name)).get()
        return queries

    def _get_ads_stats_queries(self):
        queries = self._get_range_queries(self.AD_STATS_RANGE_FIELDS, Sections.ADS_STATS)
        return queries

    def _get_range_queries(self, fields, section):
        query = Q("bool")
        for key in fields:
            field = f"{section}.{key}"
            try:
                params = str(self._params[key])
                lower_bound, upper_bound = params.replace(" ", "").split(",")
            except (KeyError, AttributeError, ValueError):
                pass
            else:
                if lower_bound and upper_bound and float(lower_bound) > float(upper_bound):
                    raise ValueError(f"Lower bound must be less than upper bound. {key}: {params}")
                if lower_bound:
                    query &= QueryBuilder().build().must().range().field(field).gte(lower_bound).get()
                if upper_bound:
                    query &= QueryBuilder().build().must().range().field(field).lte(upper_bound).get()
        return query

    def _get_terms_query(self, terms, field, operator="must"):
        terms_query = getattr(QueryBuilder().build(), operator)().terms().field(field).value(terms).get()
        return terms_query

    def _get_video_channels_subscribers_query(self, current_query):
        """
        This function creates an empty query or a query to filter out the videos whose channels have less number of
        subscribers than the desired one in the filters parameters.
        it should be only applied to Video Segments when the number of channel-subscribers is provided.
        """
        video_channels_subscribers_query = Q("bool")
        if self._params.get("segment_type") == SegmentTypeEnum.VIDEO.value and self._params.get("minimum_subscribers"):
            # Get the list of unique channel Ids using the current query
            video_manager = VideoManager(sections=(Sections.MAIN, Sections.CHANNEL))
            current_channel_ids_set = set()
            for video in video_manager.scan(filters=current_query):
                current_channel_ids_set.add(video.channel.id)

            if len(current_channel_ids_set) > 0:
                # Get the list of channel Ids that satisfy the subscribers count from the channel Ids we found above
                channel_manager = ChannelManager(sections=(Sections.MAIN, Sections.STATS))
                current_channel_ids_list = list(current_channel_ids_set)
                min_subs_ct_queries = self.get_numeric_include_na_queries(
                    attr_name="minimum_subscribers",
                    flag_name="minimum_subscribers_include_na",
                    field_name="stats.subscribers"
                )
                min_subs_ct_queries &= channel_manager.ids_query(ids=current_channel_ids_list)
                filtered_channel_ids_list = []
                for channel in channel_manager.scan(filters=min_subs_ct_queries):
                    filtered_channel_ids_list.append(channel.main.id)
                video_channels_subscribers_query = QueryBuilder().build().must().terms().field(
                                                        VIDEO_CHANNEL_ID_FIELD).value(filtered_channel_ids_list).get()
        return video_channels_subscribers_query

    @staticmethod
    def map_content_categories(content_category_ids: list):
        mapping = AuditCategory.get_all(iab=True, unique=True)
        to_string = [mapping[str(_id)] for _id in content_category_ids] or []
        return to_string

    def _map_sentiment(self, sentiment: int):
        if sentiment == 1:
            threshold = 0
        elif sentiment == 2:
            threshold = 79
        elif sentiment == 3:
            threshold = 90
        elif sentiment == 4:
            threshold = 100
        else:
            threshold = None
        return threshold

    def _map_score_threshold(self, score_threshold: int):
        """
        Map blacklist severity from client to score
        :param score_threshold: int
        :return: int
        """
        threshold = map_score_threshold(score_threshold)
        return threshold
# pylint: enable=too-many-instance-attributes
