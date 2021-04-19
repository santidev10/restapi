import io
import os
from typing import Dict, List
import zipfile
import socket

from googleapiclient import http as google_http
from googleapiclient import discovery
from six.moves.urllib.request import urlopen
from contextlib import closing

from utils.exception import backoff

# Downloading SDF reports from DV360 api results im read timeouts if default timeout is set
socket.setdefaulttimeout(1200)


class DV360Connector:
    SDF_VERSION = "SDF_VERSION_5_3"

    def __init__(self, access_token, refresh_token):
        from oauth.utils.dv360 import load_credentials
        from oauth.utils.dv360 import get_discovery_resource
        credentials = load_credentials(access_token=access_token, refresh_token=refresh_token)
        self._resource = get_discovery_resource(credentials)
        self._metrics_report_service = discovery.build("doubleclickbidmanager", "v1.1", credentials=credentials)
        self.service = discovery.build("displayvideo", "v1", credentials=credentials)

    def get_insertion_orders(self, *_, **kwargs):
        """
        :param kwargs: All keyword arguments will be passed to list method
        :example: get_insertion_orders(advertiserId="1878225", filter="campaignId=152354")
        :return: List[dict]
        """
        res = self._resource.advertisers().insertionOrders().list(**kwargs).execute()
        insertion_orders = res.get("insertionOrders", [])
        return insertion_orders

    def download_metrics_report(self, report_fp: str, report_type: str, filters: List[Dict[str, str]],
                           metrics: List[str], group_by: List[str], date_range: str, report_format="csv") -> dict:
        """
        https://developers.google.com/bid-manager/v1.1/queries
        Request and download metrics report from DV360 Double Click Bid Manager API
        Reports are generated asynchronously, and report creation status must be periodically polled from the API
        Once the API has generated the report, a download URL is provided in the completed status response and this
            method will save the report to disk to the report_fp parameter
        :param report_fp: Filepath on disk where to save completed report
        :param report_type: str -> Type of report to create from API
        :param filters: list -> Filters to apply to report_request.params.filters section
            Each filter in filters must be a dict with "type" and "value" keys
        :param metrics: list -> Metrics to apply to report
        :param group_by: list -> Filters to group data in report by
        :param date_range: str -> API Date range constant
        :param report_format: str -> Report format type
        :return: dict -> Final report response metadata

        :example:
            report_fp = "/tmp/output.csv"
            report_type = "TYPE_TRUEVIEW"
            filters = [
                {
                    "type": "FILTER_ADVERTISER",
                    "value": advertiser_id,
                },
                {
                    "type": "FILTER_INSERTION_ORDER",
                    "value": "54156"
                }
            ]
            metrics = ["METRIC_TRUEVIEW_VIEW_RATE", "METRIC_CLIENT_COST_ECPM_ADVERTISER_CURRENCY"]
            group_by = ["FILTER_ADVERTISER", "FILTER_ADVERTISER_CURRENCY", "FILTER_PLACEMENT_ALL_YOUTUBE_CHANNELS"]
            date_range = "LAST_90_DAYS"
            report_format = "csv"
        """
        report_request = {
            "params": {
                "type": report_type,
                "metrics": metrics,
                "groupBys": group_by,
                "filters": filters,
            },
            "metadata": {
                "title": "DV360 metrics report",
                "dataRange": date_range,
                "format": report_format,
            },
            "schedule": {
                "frequency": "ONE_TIME"
            }
        }
        operation = self._metrics_report_service.queries().createquery(body=report_request).execute()
        query_request = self._metrics_report_service.queries().getquery(queryId=operation["queryId"])
        response = self._poll_query_completion(query_request)
        report_download_url = response["metadata"]["googleCloudStoragePathForLatestReport"]
        self._download(report_fp, report_download_url)
        return response

    def get_line_items_sdf_report(self, advertiser_id, target_dir):
        report_filter = {
            "version": self.SDF_VERSION,
            "advertiserId": advertiser_id,
            "parentEntityFilter": {
                "fileType": "FILE_TYPE_LINE_ITEM",
                "filterType": "FILTER_TYPE_NONE",
            }
        }
        self.get_sdf_report(report_filter, target_dir)
        sdf_fp = f"{target_dir}/SDF-LineItems.csv"
        return sdf_fp

    def get_adgroup_sdf_report(self, advertiser_id, target_dir):
        report_filter = {
            "version": self.SDF_VERSION,
            "advertiserId": advertiser_id,
            "parentEntityFilter": {
                "fileType": "FILE_TYPE_AD_GROUP",
                "filterType": "FILTER_TYPE_NONE",
            }
        }
        self.get_sdf_report(report_filter, target_dir)
        ad_group_sdf_fp = f"{target_dir}/SDF-Adgroups.csv"
        return ad_group_sdf_fp

    def get_sdf_report(self, report_filter, target_dir):
        operation = self.service.sdfdownloadtasks().create(body=report_filter).execute()
        query_request = self.service.sdfdownloadtasks().operations().get(name=operation["name"])
        response = self._poll_sdf_completion(query_request)
        files = self.download_sdf(response, target_dir)
        return files

    def download_sdf(self, response, target_dir):
        resource_name = response["response"]["resourceName"]
        request = self.service.media().download_media(resourceName=resource_name)

        zipped_fp = f"{target_dir}/temp.zip"
        downloader = google_http.MediaIoBaseDownload(io.FileIO(zipped_fp, mode="wb"), request, chunksize=1024*1024)
        download_finished = False
        while download_finished is False:
            _, download_finished = downloader.next_chunk()

        with zipfile.ZipFile(zipped_fp, "r") as zip_file:
            zip_file.extractall(target_dir)

        files = os.listdir(target_dir)
        return files

    @backoff(max_backoff=3600 * 5, exceptions=(KeyError,))
    def _poll_sdf_completion(self, request):
        response = request.execute()
        if response["done"]:
            return response

    @backoff(max_backoff=3600 * 5, exceptions=(ValueError,))
    def _poll_query_completion(self, query_request):
        response = query_request.execute()
        if response["metadata"]["running"] is True or response["metadata"].get("googleCloudStoragePathForLatestReport") \
                is None:
            raise ValueError
        return response

    def _download(self, fp, url):
        with open(fp, "wb") as file, \
                closing(urlopen(url)) as url:
            file.write(url.read())
