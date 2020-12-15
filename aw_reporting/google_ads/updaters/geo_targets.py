import logging

from aw_reporting.google_ads.constants import GEO_TARGET_CONSTANT_FIELDS
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import Account
from aw_reporting.models import GeoTarget
from utils.utils import chunks_generator


logger = logging.getLogger(__name__)


class GeoTargetUpdater(UpdateMixin):
    RESOURCE_NAME = "geo_target_constant"
    UPDATE_FIELDS = ["canonical_name", "country_code", "name", "parent_id", "status", "target_type"]
    COMPARE_FIELDS = ["canonical_name", "country_code", "id", "name", "parent_id", "status", "target_type"]

    def __init__(self):
        self.client = None
        self.mcc_account = None
        self.ga_service = None
        self.geo_target_status_enum = None

    def update(self, client):
        self.client = client
        self.mcc_account = Account.objects.get(id=self.client.login_customer_id)
        self.ga_service = client.get_service("GoogleAdsService", version="v6")
        self.geo_target_status_enum = client.get_type("GeoTargetConstantStatusEnum",
                                                      version="v6").GeoTargetConstantStatus
        geo_target_constants = self._get_geo_target_constants()
        update_with_errors = []
        create_with_errors = []

        for batch in chunks_generator(geo_target_constants, size=5000):
            to_create = []
            to_update = []
            rows = list(self._parse_rows(batch))
            existing_geo_targets = {
                target["id"]: target for target
                in GeoTarget.objects.filter(id__in=[r["id"] for r in rows])
                .values("id", "name", "canonical_name", "country_code", "target_type", "status", "parent_id")
            }
            existing_parents = set(
                GeoTarget.objects
                    .filter(id__in=[g["parent_id"] for g in rows if g["parent_id"] is not None])
                    .values_list("id", flat=True)
            )
            for row in rows:
                if row["parent_id"] is not None and row["parent_id"] not in existing_parents:
                    # Parent not found in current batch, retry update after new GeoTargets have been created
                    container = update_with_errors if row["id"] in existing_geo_targets else create_with_errors
                    container.append(row)
                    continue
                geo_obj = GeoTarget(**row)
                
                if geo_obj.id in existing_geo_targets:
                    incoming_values = {key: row[key] for key in self.COMPARE_FIELDS}
                    existing_values = {key: existing_geo_targets.get(geo_obj.id, {}).get(key) for key in
                                       self.COMPARE_FIELDS}
                    if incoming_values != existing_values:
                        to_update.append(geo_obj)
                else:
                    to_create.append(geo_obj)
                
            GeoTarget.objects.bulk_create(to_create)
            GeoTarget.objects.bulk_update(to_update, self.UPDATE_FIELDS)

        # Retry updating missed geo targets after creating new geo targets
        GeoTarget.objects.bulk_create(create_with_errors)
        GeoTarget.objects.bulk_update(update_with_errors, self.UPDATE_FIELDS)

    def _get_geo_target_constants(self):
        query_fields = self.format_query(GEO_TARGET_CONSTANT_FIELDS)
        query = "SELECT {} FROM geo_target_constant".format(query_fields)
        geo_target_constants = self.ga_service.search(str(self.mcc_account.id), query=query)
        return geo_target_constants

    def _parse_rows(self, geo_target_constants):
        for row in geo_target_constants:
            geo_target_constant = row.geo_target_constant
            data = {
                "id": geo_target_constant.id,
                "name": geo_target_constant.name,
                "canonical_name": geo_target_constant.canonical_name,
                "country_code": geo_target_constant.country_code,
                "parent_id": geo_target_constant.parent_geo_target.split("/")[-1] or None,
                "target_type": geo_target_constant.target_type,
                "status": self.geo_target_status_enum.Name(geo_target_constant.status)
            }
            try:
                data["parent_id"] = int(data["parent_id"])
            except (ValueError, TypeError):
                pass
            yield data
