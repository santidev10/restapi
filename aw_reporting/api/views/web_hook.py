from datetime import datetime
from datetime import timedelta
from django.db.models import Case
from django.db.models import IntegerField
from django.db.models import Max
from django.db.models import Value
from django.db.models import Q
from django.db.models import When
from django.db.models.functions import Coalesce
from django.shortcuts import get_object_or_404
from rest_framework.response import Response
from rest_framework.views import APIView

from aw_reporting.models import Account
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignAgeRangeTargeting
from aw_reporting.models import CampaignGenderTargeting
from aw_reporting.models import CampaignLocationTargeting
from aw_reporting.models import AgeRangeOptions
from aw_reporting.models import GenderOptions
from utils.datetime import now_in_default_tz


class WebHookAWAccountsListApiView(APIView):
    permission_classes = tuple()

    @staticmethod
    def get(*_, pk, **k):
        # all customer accounts of that manager
        accounts_filter = Q(managers__id=pk) \
                        | Q(managers__managers__id=pk) \
                        | Q(managers__managers__managers__id=pk)
        account_ids = Account.objects.filter(accounts_filter)\
                                     .distinct("pk")\
                                     .values_list("pk", flat=True)
        queryset = Account.objects.filter(id__in=account_ids)
        now = now_in_default_tz()
        today = now.date()
        queryset = queryset.annotate(max_start=Coalesce(Max("campaigns__start_date"), today))
        queryset = queryset.annotate(
            priority=Case(
                When(
                    # settings have never been pulled
                    update_time__isnull=True,
                    then=Value(0),
                ),
                When(
                    # settings of newest accounts were updated more than a hour ago
                    max_start__gte=today - timedelta(days=7),
                    update_time__lte=now - timedelta(hours=1),
                    then=Value(1),
                ),
                When(
                    # other accounts will be updated once a day
                    # this update will probably run in the night (when the date change)
                    update_time__date__lt=today,
                    then=Value(2),
                ),
                # else is NULL
                output_field=IntegerField()
            )
        )
        # exclude accounts that we don't want to be updated
        queryset = queryset.exclude(priority__isnull=True)

        # priority + from newest to oldest
        queryset = queryset.order_by("priority", "-max_start", "update_time")

        # get only 50 top ids, because 50 is an AdWords Script limit
        data = queryset[:50].values_list("id", flat=True)
        return Response(data=data)


class WebHookAWSaveSettingsApiView(APIView):
    permission_classes = tuple()

    @staticmethod
    def put(request, pk, **_):
        account = get_object_or_404(Account, pk=pk)
        queryset = account.campaigns.all()

        setup_settings_fields = (
            "targeting_interests", "targeting_topics", "targeting_keywords",
            "targeting_channels", "targeting_videos",
            "targeting_remarketings", "targeting_custom_affinity",
            "tracking_template_is_set",
            "targeting_excluded_channels", "targeting_excluded_topics", "targeting_excluded_keywords",
        )

        for data in request.data["campaigns"]:
            cid = data["id"]
            setup_settings = {k: data[k] for k in setup_settings_fields}

            try:
                campaign = queryset.get(pk=cid)
            except Campaign.DoesNotExist:
                start, end = data["start"], data["end"]
                campaign = Campaign.objects.create(
                    id=cid,
                    account_id=pk,
                    name=data["name"],
                    start_date=datetime(start["year"], start["month"], start["day"]) if start else None,
                    end_date=datetime(end["year"], end["month"], end["day"]) if end else None,
                    **setup_settings
                )
            else:
                queryset.filter(pk=cid).update(**setup_settings)

            # age ranges
            age_qs = CampaignAgeRangeTargeting.objects.filter(campaign=campaign)
            age_range_ids = set(AgeRangeOptions.index(a) for a in data["age_ranges"])
            prev_age_range_ids = set(age_qs.values_list("age_range_id", flat=True))
            if prev_age_range_ids - age_range_ids:
                age_qs.exclude(age_range_id__in=age_range_ids).delete()
            insert_age_ranges = [
                CampaignAgeRangeTargeting(campaign=campaign, age_range_id=uid)
                for uid in age_range_ids - prev_age_range_ids]
            if insert_age_ranges:
                CampaignAgeRangeTargeting.objects.bulk_create(insert_age_ranges)

            # genders
            gender_qs = CampaignGenderTargeting.objects.filter(campaign=campaign)
            gender_ids = set(GenderOptions.index(g) for g in data["genders"])
            prev_gender_ids = set(gender_qs.values_list("gender_id", flat=True))
            if prev_gender_ids - gender_ids:
                gender_qs.exclude(gender_id__in=gender_ids).delete()
            insert_genders = [
                CampaignGenderTargeting(campaign=campaign, gender_id=uid)
                for uid in gender_ids - prev_gender_ids]
            if insert_genders:
                CampaignGenderTargeting.objects.bulk_create(insert_genders)

            # locations
            locations_qs = CampaignLocationTargeting.objects.filter(campaign=campaign)
            location_ids = set(data["locations"])
            prev_location_ids = set(
                locations_qs.values_list("location_id", flat=True))
            if prev_location_ids - location_ids:
                locations_qs.exclude(location_id__in=location_ids).delete()
            insert_locations = [
                CampaignLocationTargeting(campaign=campaign, location_id=uid)
                for uid in location_ids - prev_location_ids]
            if insert_locations:
                CampaignLocationTargeting.objects.bulk_create(insert_locations)

        account.settings_updated_time = now_in_default_tz
        account.save()
        return Response()
