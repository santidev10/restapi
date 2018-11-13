import csv
import logging
from collections import namedtuple

logger = logging.getLogger(__name__)


def load_google_geo_targets():
    from aw_reporting.models import GeoTarget

    ids = set(GeoTarget.objects.values_list("id", flat=True))
    logger.debug("Loading google geo targets...")
    bulk_data = []
    with open("aw_campaign/fixtures/google/geo_locations.csv") as f:
        reader = csv.reader(f, delimiter=",")
        fields = next(reader)
        row = namedtuple("Row", [f.replace(" ", "") for f in fields])
        for row_data in reader:
            if not row_data:
                continue

            r = row(*row_data)
            if int(r.CriteriaID) not in ids:
                bulk_data.append(
                    GeoTarget(
                        id=r.CriteriaID,
                        name=r.Name,
                        canonical_name=r.CanonicalName,
                        parent_id=r.ParentID
                        if r.ParentID != "0" else None,
                        country_code=r.CountryCode,
                        target_type=r.TargetType,
                        status=r.Status,
                    )
                )
    if bulk_data:
        logger.debug("Saving %d new geo targets..." % len(bulk_data))
        GeoTarget.objects.bulk_create(bulk_data)
