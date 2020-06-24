from rest_framework.generics import GenericAPIView
from rest_framework.response import Response

from aw_creation.models import AccountCreation
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign


class AwCreationChangeStatusAPIView(GenericAPIView):
    permission_classes = tuple()

    # pylint: disable=too-many-nested-blocks
    @staticmethod
    def patch(request, account_id, **_):
        updated_at = request.data.get("updated_at")
        AccountCreation.objects.filter(
            account_id=account_id, is_managed=True,
        ).update(sync_at=updated_at)
        CampaignCreation.objects.not_empty().filter(
            account_creation__account_id=account_id,
            account_creation__is_managed=True,
        ).update(sync_at=updated_at)
        AdGroupCreation.objects.not_empty().filter(
            campaign_creation__account_creation__account_id=account_id,
            campaign_creation__account_creation__is_managed=True,
        ).update(sync_at=updated_at)
        AdCreation.objects.not_empty().filter(
            ad_group_creation__campaign_creation__account_creation__account_id=account_id,
            ad_group_creation__campaign_creation__account_creation__is_managed=True,
        ).update(sync_at=updated_at)

        # save campaigns and ad_groups
        campaigns = request.data.get("campaigns", [])
        existed_c_ids = set(
            Campaign.objects.filter(account_id=account_id).values_list("id",
                                                                       flat=True))
        existed_a_ids = set(AdGroup.objects.filter(
            campaign__account_id=account_id).values_list("id", flat=True))
        c_bulk = [Campaign(id=c["id"], name=c["name"], account_id=account_id)
                  for c in campaigns if c["id"] not in existed_c_ids]
        if c_bulk:
            Campaign.objects.bulk_create(c_bulk)

        a_bulk = [AdGroup(id=a["id"], name=a["name"], campaign_id=c["id"])
                  for c in campaigns for a in c["ad_groups"]
                  if a["id"] not in existed_a_ids]
        if a_bulk:
            AdGroup.objects.bulk_create(a_bulk)

        # adding relations between campaign creations and campaign objects
        for account_creation in AccountCreation.objects.filter(account_id=account_id, is_managed=True):
            campaign_creations = account_creation.campaign_creations.all() \
                .values("id", "campaign_id")
            matched_campaign_ids = set(
                c["campaign_id"] for c in campaign_creations if
                c["campaign_id"])
            for campaign_creation in filter(lambda c: not c["campaign_id"],
                                            campaign_creations):
                # match campaign creations
                uid_key = "#{}".format(campaign_creation["id"])
                # pylint: disable=cell-var-from-loop
                filtered_campaigns = filter(lambda c: c["id"] not in matched_campaign_ids, campaigns)
                # pylint: enable=cell-var-from-loop
                for campaign in filtered_campaigns:
                    if campaign["name"].endswith(uid_key):
                        CampaignCreation.objects.filter(id=campaign_creation["id"]) \
                            .update(campaign_id=campaign["id"])
                        break

            ad_group_creations = AdGroupCreation.objects.filter(
                campaign_creation__account_creation=account_creation
            ).values("id", "ad_group_id", "campaign_creation__campaign_id")
            matched_ad_group_ids = tuple(set(
                c["ad_group_id"] for c in ad_group_creations
                if c["ad_group_id"]))
            # pylint: disable=cell-var-from-loop
            for ad_group_creation in filter(lambda a: not a["ad_group_id"], ad_group_creations):
                uid_key = "#{}".format(ad_group_creation["id"])
                filtered_campaigns = filter(lambda c: ad_group_creation["campaign_creation__campaign_id"] == c["id"],
                                            campaigns)
                for campaign in filtered_campaigns:
                    filtered_ad_groups = filter(lambda a: a["id"] not in matched_ad_group_ids,
                                                campaign["ad_groups"])
                    for ad_group in filtered_ad_groups:
                        if ad_group["name"].endswith(uid_key):
                            AdGroupCreation.objects.filter(id=ad_group_creation["id"]) \
                                .update(ad_group_id=ad_group["id"])
                            break
            # pylint: enable=cell-var-from-loop
        return Response("Successfully updated Campaign: {}".format(str(account_id)))
    # pylint: enable=too-many-nested-blocks
