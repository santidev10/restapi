from collections import defaultdict
from numbers import Number
from utils.lang import pick_dict

from django.db.models import Avg
from django.db.models import Case
from django.db.models import ExpressionWrapper
from django.db.models import F
from django.db.models import FloatField
from django.db.models import Max
from django.db.models import Min
from django.db.models import Sum
from django.db.models import When
from rest_framework.fields import SerializerMethodField

from aw_creation.api.serializers import DashboardAccountCreationListSerializer
from aw_creation.models import AccountCreation
from aw_reporting.models import dict_add_calculated_stats
from aw_reporting.models import dict_norm_base_stats
from aw_reporting.models.ad_words.calculations import DASHBOARD_MANAGED_SERVICE_CALCULATED_STATS
from aw_reporting.models.ad_words.calculations import dashboard_managed_service_aggregator
from aw_reporting.models.ad_words.campaign import Campaign


class DashboardManagedServiceSerializer(DashboardAccountCreationListSerializer):
    completion_rate = SerializerMethodField()
    viewability = SerializerMethodField()
    aw_cid = SerializerMethodField()

    class Meta:
        model = AccountCreation
        fields = (
            "ctr",
            "ctr_v",
            "id",
            "aw_cid", # account id
            "name",
            "thumbnail",
            "viewability",
            "video_view_rate",
            "completion_rate",
        )

    def _get_stats(self, account_creation_ids):
        stats = {}
        # stats by AccountCreation: viewability and completion rate
        creations = AccountCreation.objects.filter(id__in=account_creation_ids) \
            .annotate(
                completion_rate=ExpressionWrapper(
                    Sum("account__campaigns__statistics__video_views_100_quartile") / Sum("account__campaigns__statistics__impressions"),
                    output_field=FloatField(),
                ),
                viewability=Avg(
                    Case(
                        When(
                            account__campaigns__active_view_viewability__gt=0,
                            then=F("account__campaigns__active_view_viewability")
                        ),
                        output_field=FloatField(),
                    )
                ),
            )
        for creation in creations:
            creation_stats = stats.get(creation.id, {})
            creation_stats['completion_rate'] = creation.completion_rate
            creation_stats['viewability'] = creation.viewability
            stats[creation.id] = creation_stats

        # for view rate
        video_views_impressions = defaultdict(lambda: defaultdict(int))
        queryset = Campaign.objects.filter(**{
            self.CAMPAIGN_ACCOUNT_ID_KEY + "__in": account_creation_ids
        })

        with_ag_type = queryset.annotate(ag_type=Max("ad_groups__type"))
        for campaign in with_ag_type:
            creation_id = campaign.account.account_creation.id
            if campaign.ag_type == "In-stream":
                video_views_impressions[creation_id]["impressions"] += campaign.impressions
                video_views_impressions[creation_id]["views"] += campaign.video_views

        queryset = queryset \
            .values(self.CAMPAIGN_ACCOUNT_ID_KEY) \
            .order_by(self.CAMPAIGN_ACCOUNT_ID_KEY)

        dates = queryset.annotate(
            statistic_min_date=Min("statistics__date"),
            statistic_max_date=Max("statistics__date"),
        )
        dates_by_id = {
            item[self.CAMPAIGN_ACCOUNT_ID_KEY]: pick_dict(item, ["statistic_min_date", "statistic_max_date"])
            for item in dates
        }

        data = queryset.annotate(
            start=Min("start_date"),
            end=Max("end_date"),
            **dashboard_managed_service_aggregator()
        )
        for account_data in data:
            account_id = account_data[self.CAMPAIGN_ACCOUNT_ID_KEY]
            account_data.update(dates_by_id[account_id])
            dict_norm_base_stats(account_data)
            account_data["video_views"] = video_views_impressions.get(account_id, {}) \
                .get("video_views", account_data["video_views"])
            account_data["video_impressions"] = video_views_impressions.get(account_id, {}) \
                .get("video_impressions", account_data["video_impressions"])
            dict_add_calculated_stats(account_data, calculated_stats=DASHBOARD_MANAGED_SERVICE_CALCULATED_STATS)
            creation_stats = stats.get(account_id, {})
            creation_stats.update(account_data)
            stats[account_id] = creation_stats

        return stats

    def _get_daily_chart(self, account_creation_ids):
        """override parent"""
        pass

    @staticmethod
    def add_to_creation_stats(stats, creation_id, key, value):
        creation = stats.get(creation_id, {})
        values = creation.get(key, [])
        values.append(value)
        stats[creation_id][key] = values
        return stats

    def get_completion_rate(self, instance):
        value = self.stats.get(instance.id, {}).get('completion_rate', None)
        if value:
            return value * 100
        return None

    def get_viewability(self, instance):
        value = self.stats.get(instance.id, {}).get('viewability', None)
        return value

    def get_aw_cid(self, instance):
        """
        an aw_cid refers to an Account, or AccountCreation.account
        """
        return instance.account_id