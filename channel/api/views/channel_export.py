from rest_framework.views import APIView

from channel.tasks.export_data import export_channels_data
from userprofile.constants import StaticPermissions
from utils.api.mutate_query_params import VettingAdminAggregationsMixin
from utils.datetime import now_in_default_tz
from utils.es_components_exporter import ESDataS3ExportApiView
from utils.permissions import BrandSafetyDataVisible


class ChannelListExportApiView(VettingAdminAggregationsMixin, ESDataS3ExportApiView, APIView):
    permission_classes = (
        StaticPermissions.has_perms(StaticPermissions.RESEARCH__EXPORT),
    )
    generate_export_task = export_channels_data

    def _get_query_params(self, request):
        if not BrandSafetyDataVisible().has_permission(request):

            if "brand_safety" in request.query_params:
                # pylint: disable=protected-access
                request.query_params._mutable = True
                request.query_params["brand_safety"] = None
                request.query_params._mutable = False
                # pylint: enable=protected-access

            self.guard_vetting_admin_aggregations()
        request = self._add_mandatory_filters(request)
        return super(ChannelListExportApiView, self)._get_query_params(request)

    def _add_mandatory_filters(self, request):
        """ Add mandatory filters to channel research export """
        request.query_params._mutable = True
        # custom_properties.blocklist is a must_not_terms_filter, so this will query for channels that
        # must not have a value of custom_properties.blocklist = true
        request.query_params["custom_properties.blocklist"] = "true"
        request.query_params._mutable = False
        return request

    @staticmethod
    def get_filename(name):
        return f"Channels export report {name}.csv"

    @staticmethod
    def get_report_prefix():
        return f'research_channels_export_{now_in_default_tz().date().strftime("%Y_%m_%d")}'
