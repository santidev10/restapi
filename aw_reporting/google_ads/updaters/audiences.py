import csv
import itertools
import logging

import requests

from aw_reporting.models import Audience

logger = logging.getLogger(__name__)


class AudienceAWLink:
    AFFINITY = "https://developers.google.com/adwords/api/docs/appendix/affinity-categories.tsv"
    IN_MARKETING = "https://developers.google.com/adwords/api/docs/appendix/in-market-categories.tsv"


def update_audiences():
    all_data = itertools.chain(
        get_audience_data_by_link(AudienceAWLink.AFFINITY, Audience.AFFINITY_TYPE),
        get_audience_data_by_link(AudienceAWLink.IN_MARKETING, Audience.IN_MARKET_TYPE),
    )
    for audience_data in all_data:
        Audience.objects.update_or_create(id=audience_data["id"], defaults=audience_data)

    update_parents()


def get_audience_data_by_link(link, audience_type):
    response = requests.get(link)
    lines = response.text.splitlines()
    reader = csv.DictReader(lines, dialect='excel-tab')
    for row in reader:
        yield dict(
            id=row["Criterion ID"],
            name=row["Category"],
            type=audience_type,
        )


def update_parents():
    audiences = Audience.objects.filter(parent__isnull=True).order_by('id')
    for audience in audiences:
        if audience.name.count("/") > 1:
            parent_name = "/".join(
                audience.name.split("/")[:-1]
            )
            parent = Audience.objects.get(name=parent_name, type=audience.type)
            audience.parent = parent
            audience.save()
