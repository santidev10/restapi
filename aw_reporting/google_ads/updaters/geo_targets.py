import logging

from aw_reporting.google_ads.constants import GEO_TARGET_CONSTANT_FIELDS
from aw_reporting.google_ads.update_mixin import UpdateMixin
from aw_reporting.models import Account
from aw_reporting.models import GeoTarget

logger = logging.getLogger(__name__)


class GeoTargetUpdater(UpdateMixin):
    RESOURCE_NAME = "geo_target_constant"
    UPDATE_BATCH_SIZE = 10000
    UPDATE_FIELDS = ["canonical_name", "parent_id", "country_code", "target_type", "status"]

    def __init__(self):
        self.client = None
        self.mcc_account = None
        self.ga_service = None
        # Dict value for lookup by name since Google Ads API does not provide parent_id values, only parent name
        # strings in canonical_name field
        self.existing_geo_targets = {
            target.name: target.id for target in GeoTarget.objects.all()
        }
        self.existing_geo_target_ids = set(GeoTarget.objects.values_list("id", flat=True))
        self.to_update = []
        self.to_create = []
        self.with_errors = []

    def update(self, client):
        self.client = client
        self.mcc_account = Account.objects.get(id=self.client.login_customer_id)
        self.ga_service = client.get_service("GoogleAdsService", version="v2")
        self.geo_target_status_enum = client.get_type("GeoTargetConstantStatusEnum",
                                                      version="v2").GeoTargetConstantStatus
        geo_target_constants = self._get_geo_target_constants()

        for row in self._parse_rows(geo_target_constants):
            canonical_name = row["canonical_name"]
            # If comma in canonical_name and name field does not equal canonical_name field, then contains parent name
            if "," in canonical_name and row["name"] != canonical_name:
                parent_name = canonical_name.split(",")[1]
                try:
                    row["parent_id"] = self.existing_geo_targets[parent_name]
                except KeyError:
                    # Parent not found, retry update after new GeoTargets have been created
                    row["parent_name"] = parent_name
                    self.with_errors.append(row)
                    continue
            else:
                row["parent_id"] = None

            geo_obj = GeoTarget(**row)
            # Check to create or update by id
            if geo_obj.id not in self.existing_geo_target_ids:
                self.to_create.append(geo_obj)
            else:
                self.to_update.append(geo_obj)

        GeoTarget.objects.bulk_create(self.to_create)
        GeoTarget.objects.bulk_update(self.to_update, self.UPDATE_FIELDS, batch_size=self.UPDATE_BATCH_SIZE)
        self.to_update.clear()
        # Retry updating missed geo targets after creating new geo targets
        for target in self.with_errors:
            try:
                parent_name = target.pop("parent_name")
                target["parent_id"] = GeoTarget.objects.get(name=parent_name).id
                self.to_update.append(target)
            except GeoTarget.DoesNotExist:
                logger.error(f"Unable to UPDATE object with data: {target}")
        GeoTarget.objects.bulk_update(self.to_update, self.UPDATE_FIELDS, batch_size=self.UPDATE_BATCH_SIZE)

    def _get_geo_target_constants(self):
        query_fields = self.format_query(GEO_TARGET_CONSTANT_FIELDS)
        query = "SELECT {} FROM geo_target_constant".format(query_fields)
        geo_target_constants = self.ga_service.search(self.mcc_account.id, query=query)
        return geo_target_constants

    def _parse_rows(self, geo_target_constants):
        for row in geo_target_constants:
            geo_target_constant = row.geo_target_constant
            data = {
                "id": geo_target_constant.id.value,
                "name": geo_target_constant.name.value,
                "canonical_name": geo_target_constant.canonical_name.value,
                "country_code": geo_target_constant.country_code.value,
                "target_type": geo_target_constant.target_type.value,
                "status": self.geo_target_status_enum.Name(geo_target_constant.status)
            }
            yield data
