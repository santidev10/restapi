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

    tag_field_names = AdCreation.tag_field_names
    is_changed_fields = AdCreation.tag_changes_field_names
    all_tags_fields = is_changed_fields + tag_field_names
    delete_sign = "[deleted]"

    ads_data = AdCreation.objects.filter(
        ad_group_creation__campaign_creation__account_creation=account_creation,
        is_deleted=False,
        ad_group_creation__is_deleted=False,
        ad_group_creation__campaign_creation__is_deleted=False,
        ad_group_creation__campaign_creation__account_creation__is_deleted=False,
    ).exclude(**{f: False for f in is_changed_fields}).values(
        "id", "name", "ad_group_creation__id",  "ad_group_creation__name",
        "ad_group_creation__campaign_creation__id", "ad_group_creation__campaign_creation__name",
        *all_tags_fields
    )
    if ads_data:
        campaigns = defaultdict(lambda: defaultdict(dict))
        ad_ids = set()
        for ad in ads_data:
            campaign_hash = "{} #{}".format(
                ad["ad_group_creation__campaign_creation__name"], ad["ad_group_creation__campaign_creation__id"],
            )
            ad_group_hash = "{} #{}".format(ad["ad_group_creation__name"], ad["ad_group_creation__id"])
            ad_hash = "{} #{}".format(ad["name"], ad["id"])

            tags = ((t, ad[t] if ad[t] else delete_sign)
                    for t in tag_field_names
                    if ad[t] or ad["{}_changed".format(t)])
            campaigns[campaign_hash][ad_group_hash][ad_hash] = [dict(name=tag_field_to_readable_name(name), url=url)
                                                                for name, url in tags]
            ad_ids.add(ad["id"])

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

        # drop the changes flags
        AdCreation.objects.filter(id__in=ad_ids).update(**{f: False for f in is_changed_fields})
