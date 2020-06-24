import calendar
from collections import OrderedDict

from django.db.models import Case
from django.db.models import IntegerField as AggrIntegerField
from django.db.models import Value
from django.db.models import When
from django.templatetags.static import static
from drf_yasg.utils import swagger_auto_schema
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.views import APIView

from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_creation.models import FrequencyCap
from aw_creation.models import Language
from aw_creation.models import LocationRule
from aw_creation.models.creation import BUDGET_TYPE_CHOICES
from .schemas import CREATION_OPTIONS_SCHEMA


class CreationOptionsApiView(APIView):
    @swagger_auto_schema(
        operation_description="Allowed options for account creations",
        responses={
            HTTP_200_OK: CREATION_OPTIONS_SCHEMA,
        },
    )
    def get(self, request, **k):
        def opts_to_response(opts):
            res = [dict(id=i, name=n) for i, n in opts]
            return res

        def list_to_resp(l, n_func=None):
            n_func = n_func or (lambda e: e)
            return [dict(id=i, name=n_func(i)) for i in l]

        def get_week_day_name(n):
            return calendar.day_name[n - 1]

        ad_schedule_rules = list_to_resp(range(1, 8), n_func=get_week_day_name)
        additional_ad_schedule_rules = [
            {"id": 8, "name": "All Days"},
            {"id": 9, "name": "Weekdays"},
            {"id": 10, "name": "Weekends"},
        ]
        ad_schedule_rules.extend(additional_ad_schedule_rules)

        options = OrderedDict(
            # ACCOUNT
            # create
            name="string;max_length=250;required;validation=^[^#']*$",
            # create and update
            video_ad_format=[
                dict(
                    id=i, name=n,
                    thumbnail=request.build_absolute_uri(
                        static("img/{}.png".format(i)))
                )
                for i, n in AdGroupCreation.VIDEO_AD_FORMATS
            ],
            # update
            goal_type=opts_to_response(
                CampaignCreation.GOAL_TYPES[:1],
            ),
            type=opts_to_response(
                CampaignCreation.CAMPAIGN_TYPES,
            ),
            bidding_type=opts_to_response(
                CampaignCreation.BIDDING_TYPES,
            ),
            delivery_method=opts_to_response(
                CampaignCreation.DELIVERY_METHODS[:1],
            ),
            video_networks=opts_to_response(
                CampaignCreation.VIDEO_NETWORKS,
            ),

            # CAMPAIGN
            start="date",
            end="date",
            goal_units="integer;max_value=4294967294",
            budget="decimal;max_digits=10,decimal_places=2",
            budget_type=opts_to_response(BUDGET_TYPE_CHOICES),
            max_rate="decimal;max_digits=6,decimal_places=3",
            languages=[
                dict(id=l.id, name=l.name)
                for l in Language.objects.all().annotate(
                    priority=Case(
                        When(
                            id=1000,
                            then=Value(0),
                        ),
                        default=Value(1),
                        output_field=AggrIntegerField()
                    )
                ).order_by("priority", "name")
            ],
            devices=opts_to_response(CampaignCreation.DEVICES),
            content_exclusions=opts_to_response(
                CampaignCreation.CONTENT_LABELS),
            frequency_capping=dict(
                event_type=opts_to_response(FrequencyCap.EVENT_TYPES),
                level=opts_to_response(FrequencyCap.LEVELS),
                limit="positive integer;max_value=65534",
                time_unit=opts_to_response(FrequencyCap.TIME_UNITS),
                __help="a list of two objects is allowed"
                       "(for each of the event types);",
            ),
            location_rules=dict(
                geo_target="the list can be requested from:"
                           " /geo_target_list/?search=Kiev",
                latitude="decimal",
                longitude="decimal",
                radius="integer;max_values are 800mi and 500km",
                radius_units=opts_to_response(LocationRule.UNITS),
                __help="a list of objects is accepted;"
                       "either geo_target_id or "
                       "coordinates and a radius - required",
            ),
            ad_schedule_rules=dict(
                day=ad_schedule_rules,
                from_hour=list_to_resp(range(0, 24)),
                from_minute=list_to_resp(range(0, 60, 15)),
                to_hour=list_to_resp(range(0, 24)),
                to_minute=list_to_resp(range(0, 60, 15)),
                __help="accepts a list of rules;example: [{'day': 1, "
                       "'from_hour': 9, 'from_minute': 15, 'to_hour': 18, "
                       "'to_minute': 45}];from_minute and to_minute are "
                       "not required",
            ),

            # AD GROUP
            video_url="url;validation=valid_yt_video",
            ct_overlay_text="string;max_length=200",
            display_url="string;max_length=200",
            final_url="url;max_length=200",
            genders=opts_to_response(
                AdGroupCreation.GENDERS,
            ),
            parents=opts_to_response(
                AdGroupCreation.PARENTS,
            ),
            age_ranges=opts_to_response(
                AdGroupCreation.AGE_RANGES,
            ),

            # Bidding Strategy
            bidding_strategy_types=opts_to_response(
                CampaignCreation.BID_STRATEGY_TYPES,
            ),
        )
        return Response(data=options)
