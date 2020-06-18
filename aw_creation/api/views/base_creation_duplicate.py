import re
from collections import defaultdict

from django.db import transaction
from django.db.models import Q
from django.http import Http404
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_creation.api.serializers import AccountCreationSetupSerializer
from aw_creation.models import AccountCreation
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import AdScheduleRule
from aw_creation.models import CampaignCreation
from aw_creation.models import FrequencyCap
from aw_creation.models import LocationRule
from aw_creation.models import TargetingItem
from aw_reporting.demo.views import forbidden_for_demo
from utils.permissions import MediaBuyingAddOnPermission


class BaseCreationDuplicateApiView(APIView):
    serializer_class = AccountCreationSetupSerializer
    permission_classes = (MediaBuyingAddOnPermission,)

    campaign_fields = (
        "name", "start", "end", "budget", "devices_raw", "delivery_method",
        "type", "bid_strategy_type",
        "video_networks_raw", "content_exclusions_raw",
        "target_cpa",
    )
    loc_rules_fields = (
        "geo_target", "latitude", "longitude", "radius", "radius_units",
        "bid_modifier",
    )
    freq_cap_fields = ("event_type", "level", "limit", "time_unit")
    ad_schedule_fields = (
        "day", "from_hour", "from_minute", "to_hour", "to_minute",
    )
    ad_group_fields = (
        "name", "max_rate", "genders_raw", "parents_raw", "age_ranges_raw",
        "video_ad_format",
    )
    ad_fields = (
        "name", "video_url", "display_url", "final_url", "tracking_template",
        "custom_params", "companion_banner",
        "video_id", "video_title", "video_description", "video_thumbnail",
        "video_channel_title", "video_duration",
        "beacon_impression_1", "beacon_impression_2", "beacon_impression_3",
        "beacon_view_1", "beacon_view_2", "beacon_view_3",
        "beacon_skip_1", "beacon_skip_2", "beacon_skip_3",
        "beacon_first_quartile_1", "beacon_first_quartile_2",
        "beacon_first_quartile_3",
        "beacon_midpoint_1", "beacon_midpoint_2", "beacon_midpoint_3",
        "beacon_third_quartile_1", "beacon_third_quartile_2",
        "beacon_third_quartile_3",
        "beacon_completed_1", "beacon_completed_2", "beacon_completed_3",
        "beacon_vast_1", "beacon_vast_2", "beacon_vast_3",
        "beacon_dcm_1", "beacon_dcm_2", "beacon_dcm_3",
        "business_name",
        "long_headline",
        "short_headline",
        "description_1",
        "description_2",
    )
    targeting_fields = ("criteria", "type", "is_negative")

    is_demo = None

    def get_queryset(self):
        raise NotImplementedError

    @forbidden_for_demo(
        lambda view, request, pk, **kwargs: view.get_queryset().filter(view.is_demo & Q(pk=pk)).exists())
    def post(self, request, pk, **kwargs):
        try:
            instance = self.get_queryset().get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        bulk_items = defaultdict(list)
        with transaction.atomic():
            duplicate = self.duplicate_item(instance, bulk_items,
                                            request.GET.get("to"))
            self.insert_bulk_items(bulk_items)

        response = self.serializer_class(duplicate).data
        return Response(data=response)

    def duplicate_item(self, item, bulk_items, to_parent):
        if isinstance(item, CampaignCreation):
            parent = item.account_creation
            return self.duplicate_campaign(parent, item, bulk_items,
                                           all_names=parent.campaign_creations.values_list(
                                               "name", flat=True))
        if isinstance(item, AdGroupCreation):
            if to_parent:
                try:
                    parent = CampaignCreation.objects \
                        .filter(account_creation__owner=self.request.user) \
                        .get(pk=to_parent)
                except CampaignCreation.DoesNotExist:
                    raise Http404
            else:
                parent = item.campaign_creation

            return self.duplicate_ad_group(
                parent, item, bulk_items,
                all_names=None if to_parent else parent.ad_group_creations.values_list(
                    "name", flat=True),
            )
        if isinstance(item, AdCreation):
            if to_parent:
                try:
                    parent = AdGroupCreation.objects.filter(
                        campaign_creation__account_creation__owner=self.request.user
                    ).get(pk=to_parent)
                except AdGroupCreation.DoesNotExist:
                    raise Http404
            else:
                parent = item.ad_group_creation
            return self.duplicate_ad(
                parent, item, bulk_items,
                all_names=None if to_parent else parent.ad_creations.values_list(
                    "name", flat=True),
            )
        raise NotImplementedError("Unknown item type: {}".format(type(item)))

    def duplicate_campaign(self, account, campaign, bulk_items,
                           all_names=None):
        camp_data = {f: getattr(campaign, f) for f in self.campaign_fields}
        c_duplicate = CampaignCreation.objects.create(
            account_creation=account, **camp_data
        )
        if all_names:
            c_duplicate.name = self.increment_name(c_duplicate.name, all_names)
            c_duplicate.save()
        # through
        language_through = CampaignCreation.languages.through
        for lid in campaign.languages.values_list("id", flat=True):
            bulk_items["languages"].append(
                language_through(campaigncreation_id=c_duplicate.id,
                                 language_id=lid)
            )

        for r in campaign.location_rules.all():
            bulk_items["location_rules"].append(
                LocationRule(
                    campaign_creation=c_duplicate,
                    **{f: getattr(r, f) for f in self.loc_rules_fields}
                )
            )

        for i in campaign.frequency_capping.all():
            bulk_items["frequency_capping"].append(
                FrequencyCap(
                    campaign_creation=c_duplicate,
                    **{f: getattr(i, f) for f in self.freq_cap_fields}
                )
            )

        for i in campaign.ad_schedule_rules.all():
            bulk_items["ad_schedule_rules"].append(
                AdScheduleRule(
                    campaign_creation=c_duplicate,
                    **{f: getattr(i, f) for f in self.ad_schedule_fields}
                )
            )

        for a in campaign.ad_group_creations.filter(is_deleted=False):
            self.duplicate_ad_group(c_duplicate, a, bulk_items)

        return c_duplicate

    def duplicate_ad_group(self, campaign, ad_group, bulk_items,
                           all_names=None):
        a_duplicate = AdGroupCreation.objects.create(
            campaign_creation=campaign,
            **{f: getattr(ad_group, f) for f in self.ad_group_fields}
        )
        if all_names:
            a_duplicate.name = self.increment_name(a_duplicate.name, all_names)
            a_duplicate.save()

        for i in ad_group.targeting_items.all():
            bulk_items["targeting_items"].append(
                TargetingItem(
                    ad_group_creation=a_duplicate,
                    **{f: getattr(i, f) for f in self.targeting_fields}
                )
            )

        for ad in ad_group.ad_creations.filter(is_deleted=False):
            self.duplicate_ad(a_duplicate, ad, bulk_items)

        return a_duplicate

    def duplicate_ad(self, ad_group, ad, *_, all_names=None):
        tag_field_names = AdCreation.tag_field_names
        data = {}
        for f in self.ad_fields:
            data[f] = getattr(ad, f)
            if f in tag_field_names and getattr(ad, f):
                data["{}_changed".format(f)] = True
        ad_duplicate = AdCreation.objects.create(ad_group_creation=ad_group,
                                                 **data)
        if all_names:
            ad_duplicate.name = self.increment_name(ad_duplicate.name,
                                                    all_names)
            ad_duplicate.save()
        return ad_duplicate

    @staticmethod
    def increment_name(name, all_names):
        len_limit = 250
        mark_match = re.match(r".*( \(\d+\))$", name)

        # clear name from mark
        if mark_match:
            mark_str = mark_match.group(1)
            name = name[:-len(mark_str)]  # crop a previous mark from a name
            # copy_number = int(mark_match.group(2)) + 1

        # find top mark number
        max_number = 0
        for n in all_names:
            n_match = re.match(r"(.*) \((\d+)\)$", n)
            if n_match and n_match.group(1) == name:
                number = int(n_match.group(2))
                if number > max_number:
                    max_number = number

        # create new name
        copy_sign = " ({})".format(max_number + 1)
        max_len = len_limit - len(copy_sign)
        if len(name) >= max_len:
            name = name[:max_len - 2] + ".."
        return name + copy_sign

    @staticmethod
    def insert_bulk_items(bulk_items):
        if bulk_items["languages"]:
            CampaignCreation.languages.through.objects.bulk_create(
                bulk_items["languages"])

        if bulk_items["location_rules"]:
            LocationRule.objects.bulk_create(bulk_items["location_rules"])

        if bulk_items["frequency_capping"]:
            FrequencyCap.objects.bulk_create(bulk_items["frequency_capping"])

        if bulk_items["ad_schedule_rules"]:
            AdScheduleRule.objects.bulk_create(bulk_items["ad_schedule_rules"])

        if bulk_items["targeting_items"]:
            TargetingItem.objects.bulk_create(bulk_items["targeting_items"])
