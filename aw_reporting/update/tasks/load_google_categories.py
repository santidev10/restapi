import csv
import logging
from collections import namedtuple

logger = logging.getLogger(__name__)

__all__ = ["load_google_categories"]


def categories_define_parents():
    from aw_reporting.models import Audience
    offset = 0
    limit = 100
    while True:
        audiences = Audience.objects.filter(
            parent__isnull=True).order_by("id")[offset:offset + limit]
        if not audiences:
            break
        for audience in audiences:
            if audience.name.count("/") > 1:
                parent_name = "/".join(
                    audience.name.split("/")[:audience.name.count("/")]
                )
                parent = Audience.objects.get(name=parent_name)
                audience.parent = parent
                audience.save()
                offset -= 1

        offset += limit


def load_google_categories(skip_audiences=False, skip_topics=False):
    from aw_reporting.models import Audience, Topic

    if not Audience.objects.count() and not skip_audiences:

        logger.debug("Loading audiences...")
        files = (
            ("affinity_categories.csv", ("ID", "Category")),
            ("in-market_categories.csv", ("ID", "Category")),
            ("verticals.csv", ("Category", "ID", "ParentID")),
        )
        bulk_data = []

        for f_name, fields in files:

            list_type = f_name.split("_")[0]
            with open("aw_campaign/fixtures/google/%s" % f_name) as f:
                content = f.read()

            reader = csv.reader(content.split("\n")[1:], delimiter=",")
            row = namedtuple("Row", fields)
            for row_data in reader:
                if not row_data:
                    continue
                r = row(*row_data)
                bulk_data.append(
                    Audience(
                        id=r.ID,
                        name=r.Category,
                        parent_id=r.ParentID if "ParentID" in fields and
                                                r.ParentID != "0" else None,
                        type=list_type,
                    )
                )
        Audience.objects.bulk_create(bulk_data)
        categories_define_parents()

    if not Topic.objects.count() and not skip_topics:
        logger.debug("Loading topics...")
        bulk_data = []
        # topics
        fields = ("Category", "ID", "ParentID")
        with open("aw_campaign/fixtures/google/verticals.csv") as f:
            content = f.read()
            reader = csv.reader(content.split("\n")[1:], delimiter=",")
            row = namedtuple("Row", fields)
            for row_data in reader:
                if not row_data:
                    continue
                r = row(*row_data)
                bulk_data.append(
                    Topic(
                        id=r.ID,
                        name=r.Category,
                        parent_id=r.ParentID if r.ParentID != "0" else None
                    )
                )
        Topic.objects.bulk_create(bulk_data)
