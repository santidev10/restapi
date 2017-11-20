from django.core.mail import send_mail
from django.conf import settings
from django.template.loader import render_to_string
from collections import defaultdict

TRACKING_TAGS_SUBJECT = "SAAS Tags Needed for {account} by {first_name} {last_name}"


def tag_field_to_readable_name(field_name):
    parts = field_name.split("_")
    if parts[1] in ('dcm', "vast"):
        parts[1] = parts[1].upper()
    else:
        parts[1] = parts[1].capitalize()
    return " ".join(parts[1:])


def send_tracking_tags_request(user, account_creation):
    from aw_creation.models import AdCreation

    tag_fields = (
        "beacon_impression_1", "beacon_impression_2", "beacon_impression_3",
        "beacon_view_1", "beacon_view_2", "beacon_view_3",
        "beacon_skip_1", "beacon_skip_2", "beacon_skip_3",
        "beacon_first_quartile_1", "beacon_first_quartile_2", "beacon_first_quartile_3",
        "beacon_midpoint_1", "beacon_midpoint_2", "beacon_midpoint_3",
        "beacon_third_quartile_1", "beacon_third_quartile_2", "beacon_third_quartile_3",
        "beacon_completed_1", "beacon_completed_2", "beacon_completed_3",
        "beacon_vast_1", "beacon_vast_2", "beacon_vast_3",
        "beacon_dcm_1", "beacon_dcm_2", "beacon_dcm_3",
    )
    ads_data = AdCreation.objects.filter(
        ad_group_creation__campaign_creation__account_creation=account_creation,
    ).exclude(**{f: "" for f in tag_fields}).values(
        "id", "name",
        "ad_group_creation__id",  "ad_group_creation__name",
        "ad_group_creation__campaign_creation__id", "ad_group_creation__campaign_creation__name",
        *tag_fields
    )
    if ads_data:
        campaigns = defaultdict(lambda: defaultdict(dict))
        for ad in ads_data:
            campaign_hash = "{} #{}".format(
                ad["ad_group_creation__campaign_creation__name"], ad["ad_group_creation__campaign_creation__id"],
            )
            ad_group_hash = "{} #{}".format(ad["ad_group_creation__name"], ad["ad_group_creation__id"])
            ad_hash = "{} #{}".format(ad["name"], ad["id"])

            tags = ((t, ad[t]) for t in tag_fields if ad[t])
            campaigns[campaign_hash][ad_group_hash][ad_hash] = [dict(name=tag_field_to_readable_name(name), url=url)
                                                                for name, url in tags]

        campaigns = [
            dict(
                name=c_name,
                ad_groups=[
                    dict(
                        name=ag_name,
                        ads=[
                            dict(name=a_name, tags=tags)
                            for a_name, tags in ads.items()
                        ]
                    )
                    for ag_name, ads in ad_groups.items()
                ]
            )
            for c_name, ad_groups in campaigns.items()
        ]

        context = dict(
            first_name=user.first_name,
            last_name=user.last_name,
            account=account_creation.name,
            cid=account_creation.account_id,
            campaigns=campaigns,
        )
        message = render_to_string("tracking_tags_message.txt", context)
        subject = TRACKING_TAGS_SUBJECT.format(**context)
        send_mail(subject, message, settings.EMAIL_HOST_USER, [settings.MS_CHANNELFACTORY_EMAIL], fail_silently=False)
