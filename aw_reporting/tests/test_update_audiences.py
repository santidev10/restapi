from contextlib import contextmanager
from functools import partial

import requests
from django.test import TestCase
from rest_framework.status import HTTP_200_OK
from unittest.mock import patch

from aw_reporting.models import Audience
from aw_reporting.google_ads.updaters.audiences import update_audiences
from aw_reporting.google_ads.updaters.audiences import AudienceAWLink
from utils.unittests.generic_test import generic_test
from utils.unittests.int_iterator import int_iterator


class UpdateAudiencesTestCase(TestCase):
    @generic_test([
        ("Affinity", (Audience.AFFINITY_TYPE,), dict()),
        ("In-marketing", (Audience.IN_MARKET_TYPE,), dict()),
    ])
    def test_audiences_by_type(self, affinity_type):
        test_affinity_data = dict(
            id=next(int_iterator),
            name="test affinity"
        )
        data_link = LINK_FOR_AUDIENCE_TYPE.get(affinity_type)
        test_audience_data = {data_link: [test_affinity_data]}

        test_queryset = Audience.objects.filter(
            **test_affinity_data,
            type=affinity_type,
        )
        self.assertFalse(test_queryset.exists())

        with path_response(test_audience_data):
            update_audiences()

        self.assertTrue(test_queryset.exists())
        self.assertEqual(test_queryset.first().name, test_affinity_data["name"])

    def test_link_parents(self):
        parent_name = "/parent name"
        test_affinity_data_1 = dict(
            id=next(int_iterator),
            name=parent_name
        )
        test_affinity_data_2 = dict(
            id=next(int_iterator),
            name=parent_name + "/test affinity"
        )
        affinity_type = Audience.AFFINITY_TYPE
        data_link = LINK_FOR_AUDIENCE_TYPE.get(affinity_type)
        test_audience_data = {
            data_link: [
                test_affinity_data_1,
                test_affinity_data_2,
            ]
        }

        with path_response(test_audience_data):
            update_audiences()

        self.assertEqual(
            Audience.objects.get(pk=test_affinity_data_2["id"]).parent_id,
            test_affinity_data_1["id"]
        )


LINK_FOR_AUDIENCE_TYPE = {
    Audience.AFFINITY_TYPE: AudienceAWLink.AFFINITY,
    Audience.IN_MARKET_TYPE: AudienceAWLink.IN_MARKETING,
}


@contextmanager
def path_response(test_data):
    partial_response = partial(test_response, test_data)
    with patch.object(requests, "get", side_effect=partial_response) as patch_response:
        yield patch_response


def test_response(test_data, url, *args, **kwargs):
    response_data = test_data.get(url, [])
    response_data_with_headers = \
        [dict(id="Criterion ID", name="Category")] \
        + response_data
    test_csv = "\r\n".join([
        "\t".join([str(item["id"]), item["name"]])
        for item in response_data_with_headers
    ])
    return MockResponse(text=test_csv)


class MockResponse(object):
    def __init__(self, status_code=HTTP_200_OK, text=None, **kwargs):
        self.status_code = status_code
        self.text = text
