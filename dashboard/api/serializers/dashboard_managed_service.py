from numbers import Number

from aw_reporting.models.salesforce_constants import SalesForceGoalType
from rest_framework.fields import SerializerMethodField
from rest_framework.serializers import Serializer
from utils.serializers.fields import PercentField
# from rest_framework.serializers import FloatField
from django.db.models import FloatField
from rest_framework.serializers import CharField
from aw_creation.api.serializers import DashboardAccountCreationListSerializer
from aw_creation.models import AccountCreation
from django.db.models import F, ExpressionWrapper, Sum, Avg
from django.db.models import Case
from django.db.models import When
from aw_reporting.models.ad_words.campaign import Campaign



class DashboardManagedServiceAveragesSerializer(DashboardAccountCreationListSerializer):
    # ctr = PercentField()
    # video_view_rate = PercentField()
    completion_rate = SerializerMethodField()
    viewability = SerializerMethodField()
    # margin = SerializerMethodField()

    class Meta:
        model = AccountCreation
        fields = (
            "ctr",
            "ctr_v",
            "id",
            "name",
            "thumbnail",
            "viewability",
            "video_view_rate",
            "completion_rate",
            # admin fields
            # "margin"
        )

    # def _fields_to_exclude(self):
    #     return (
    #         "account", "all_conversions", "average_cpm", "average_cpv",
    #         "brand", "clicks", "cost", "cost_method", "currency_code", "end",
    #         "impressions", "is_changed", "is_disapproved", "plan_cpm",
    #         "plan_cpv", "sf_account", "start", "statistic_max_date",
    #         "statistic_min_date", "status", "updated_at", "video_views",
    #         "weekly_chart",
    #     )

    def _get_stats(self, account_creation_ids):
        stats = super()._get_stats(account_creation_ids)
        # stats by AccountCreation
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
                # TODO we can remove these
                completion_views=Sum("account__campaigns__statistics__video_views_100_quartile"),
                impressions=Sum("account__campaigns__statistics__impressions"),
            )
        for creation in creations:
            stats[creation.id]['completion_rate'] = creation.completion_rate
            stats[creation.id]['viewability'] = creation.viewability

        # TODO for view rate
        # video_views_impressions = defaultdict(lambda: defaultdict(int))
        # queryset = Campaign.objects.filter(**{
        #     self.CAMPAIGN_ACCOUNT_ID_KEY + "__in": account_creation_ids
        # })
        # with_ag_type = queryset.annotate(ag_type=Max("ad_groups__type"))
        # for campaign in with_ag_type:
        #     creation_id = campaign.account.account_creation.id
        #     if campaign.ag_type == "In-stream":
        #         video_views_impressions[creation_id]["impressions"] += campaign.impressions
        #         video_views_impressions[creation_id]["views"] += campaign.video_views

        # # stats by AW Campaign
        # campaigns = Campaign.objects.filter(account__account_creation__id__in=account_creation_ids) \
        #     .values('id', 'salesforce_placement__goal_type_id',) \
        #     .exclude(salesforce_placement__goal_type_id__isnull=True) \
        #     .annotate(
        #         sum_video_views=Sum("statistics__video_views"),
        #         sum_impressions=Sum("statistics__impressions"),
        #         sum_cost=Sum("statistics__cost"),
        #         ordered_rate=F("salesforce_placement__ordered_rate"),
        #         creation_id=F("account__account_creation__id"),
        #         placement_id=F("salesforce_placement__id"),
        #     )
        # for campaign in campaigns:
        #     goal_type_id = campaign.get('salesforce_placement__goal_type_id')
        #     rate = campaign.get('ordered_rate', 0) or 0
        #     if goal_type_id == SalesForceGoalType.CPV:
        #         sum_video_views = campaign.get('sum_video_views', 0) or 0
        #         revenue = sum_video_views * rate
        #     elif goal_type_id == SalesForceGoalType.CPM:
        #         sum_impressions = campaign.get('sum_impressions', 0) or 0
        #         revenue = sum_impressions * rate / 1000 if sum_impressions and rate else 0
        #     else:
        #         revenue = 0
        #     creation_id = campaign['creation_id']
        #     # store revenues
        #     self.add_to_creation_stats(stats, creation_id, 'revenues', revenue)
        #     # store costs
        #     cost = campaign['sum_cost']
        #     self.add_to_creation_stats(stats, creation_id, 'costs', cost)

        return stats

    def _get_daily_chart(self, account_creation_ids):
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
        # return instance.account.active_view_viewability
        return value

    def get_margin(self, instance):
        revenues = self.stats.get(instance.id, {}).get('revenues', [])
        costs = self.stats.get(instance.id, {}).get('costs', [])
        revenues = [revenue for revenue in revenues if isinstance(revenue, Number)]
        costs = [cost for cost in costs if isinstance(cost, Number)]
        revenue = sum(revenues)
        cost = sum(costs)
        profit = revenue - cost
        margin = profit / revenue if revenue else None
        margin *= 100
        return margin


class DashboardManagedServiceAveragesAdminSerializer(DashboardManagedServiceAveragesSerializer):
    pass
    # margin = PercentField()
    # pacing = PercentField()
    # cpv = FloatField()


class BaseOpportunitySerializer(Serializer):
    pass
    # name = CharField(max_length=250)
    # aw_cid = CharField()


class DashboardManagedServiceOpportunitySerializer(
    BaseOpportunitySerializer,
    DashboardManagedServiceAveragesSerializer):
    pass


class DashboardManagedServiceOpportunityAdminSerializer(
    BaseOpportunitySerializer,
    DashboardManagedServiceAveragesAdminSerializer):
    pass
