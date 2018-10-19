import logging

from django.db.models import F

logger = logging.getLogger(__name__)


def add_relation_between_report_and_creation_campaigns():
    """
    We need a quick way to find out
    which aw_creation.CampaignCreation object relates to a aw_reporting.Campaign object and vice versa
    We use name convention: #{creation_object_id} in the end of names for items created on AdWords
    we will use it to find relations and link them
    :return:
    """
    from aw_creation.models import CampaignCreation
    from aw_reporting.models import Campaign

    # link campaigns
    acc_id_key = "account_creation__account_id"
    campaign_creations = CampaignCreation.objects.filter(
        account_creation__account__isnull=False,
        campaign__isnull=True,
        sync_at__gt=F("created_at"),
    ).values("id", acc_id_key).order_by(acc_id_key)

    account_id = ""
    campaigns = Campaign.objects.none()
    for c in campaign_creations:
        if account_id != c[acc_id_key]:
            account_id = c[acc_id_key]
            campaigns = Campaign.objects.filter(account_id=account_id)
        try:
            campaign = campaigns.get(name__endswith="#{}".format(c["id"]))
        except Campaign.DoesNotExist:
            pass
        except Exception as e:
            # there might be two campaigns. I want to know if this happens
            logger.critical("Unhandled: {}".format(e))
        else:
            CampaignCreation.objects.filter(pk=c["id"]).update(campaign=campaign)


def add_relation_between_report_and_creation_ad_groups():
    from aw_creation.models import AdGroupCreation
    from aw_reporting.models import AdGroup

    # link ad groups
    campaign_id_key = "campaign_creation__campaign_id"
    ad_group_creations = AdGroupCreation.objects.filter(
        campaign_creation__campaign__isnull=False,
        ad_group__isnull=True,
        sync_at__gt=F("created_at"),
    ).values("id", campaign_id_key).order_by(campaign_id_key)

    campaign_id = ""
    ad_groups = AdGroup.objects.none()
    for a in ad_group_creations:
        if campaign_id != a[campaign_id_key]:
            campaign_id = a[campaign_id_key]
            ad_groups = AdGroup.objects.filter(campaign_id=campaign_id)

        try:
            ad_group = ad_groups.get(name__endswith="#{}".format(a["id"]))
        except AdGroup.DoesNotExist:
            pass
        except Exception as e:
            logger.critical("Unhandled: {}".format(e))
        else:
            AdGroupCreation.objects.filter(pk=a["id"]).update(ad_group=ad_group)


def add_relation_between_report_and_creation_ads():
    from aw_creation.models import AdCreation
    from aw_reporting.models import Ad

    ad_group_id_key = "ad_group_creation__ad_group_id"
    ad_creations = AdCreation.objects.filter(
        ad_group_creation__ad_group__isnull=False,
        ad__isnull=True,
        sync_at__gt=F("created_at"),
    ).values("id", ad_group_id_key).order_by(ad_group_id_key)

    ad_group_id = ""
    ads = Ad.objects.none()
    for a in ad_creations:
        if ad_group_id != a[ad_group_id_key]:
            ad_group_id = a[ad_group_id_key]
            ads = Ad.objects.filter(ad_group_id=ad_group_id)

        try:
            ad = ads.get(creative_name__endswith="#{}".format(a["id"]))
        except Ad.DoesNotExist:
            pass
        except Exception as e:
            logger.critical("Unhandled: {}".format(e))
        else:
            AdCreation.objects.filter(pk=a["id"]).update(ad=ad)
