from urllib.parse import urlencode

from rest_framework.reverse import reverse
from rest_framework.status import HTTP_200_OK

from utils.utils_tests import ExtendedAPITestCase
from segment.models import SegmentKeyword, SegmentChannel, SegmentVideo


class SegmentListCreateApiViewTestCase(ExtendedAPITestCase):
    def test_keyword_list_should_be_filtered_by_average_cpc(self):
        user = self.create_test_user()
        user.is_staff = True
        user.save()

        SegmentKeyword.objects.create(average_cpc=2)
        SegmentKeyword.objects.create(average_cpc=3)
        SegmentKeyword.objects.create(average_cpc=1)
        query_params = {
            "sort_by": "average_cpc",
            "ascending": 1
        }
        url = reverse("segment_api_urls:segment_list",
                      kwargs={"segment_type": "keyword"}) \
              + "?" + urlencode(query_params)
        print(url)
        response = self.client.get(url)
        print(response.data)

        average_cpcs = [s["statistics"]["average_cpc"] for s in
                        response.data["items"]]
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(set(average_cpcs)), 3)
        self.assertEqual(average_cpcs, sorted(average_cpcs))

    def test_keyword_list_should_be_filtered_by_competition(self):
        user = self.create_test_user()
        user.is_staff = True
        user.save()

        SegmentKeyword.objects.create(competition=2)
        SegmentKeyword.objects.create(competition=3)
        SegmentKeyword.objects.create(competition=1)
        query_params = {
            "sort_by": "competition",
            "ascending": 1
        }
        url = reverse("segment_api_urls:segment_list",
                      kwargs={"segment_type": "keyword"}) \
              + "?" + urlencode(query_params)
        print(url)
        response = self.client.get(url)
        print(response.data)

        competitions = [s["statistics"]["competition"] for s in
                        response.data["items"]]
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(set(competitions)), 3)
        self.assertEqual(competitions, sorted(competitions))

    def test_keyword_list_should_be_filtered_by_average_volume(self):
        user = self.create_test_user()
        user.is_staff = True
        user.save()

        SegmentKeyword.objects.create(average_volume=2)
        SegmentKeyword.objects.create(average_volume=3)
        SegmentKeyword.objects.create(average_volume=1)
        query_params = {
            "sort_by": "average_volume",
            "ascending": 1
        }
        url = reverse("segment_api_urls:segment_list",
                      kwargs={"segment_type": "keyword"}) \
              + "?" + urlencode(query_params)
        print(url)
        response = self.client.get(url)
        print(response.data)

        average_volumes = [s["statistics"]["average_volume"] for s in
                           response.data["items"]]
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(set(average_volumes)), 3)
        self.assertEqual(average_volumes, sorted(average_volumes))

    def test_segment_list_updated_at_field(self):
        user = self.create_test_user()
        user.is_staff = True
        user.save()
        segment_instances = [
            SegmentChannel.objects.create(),
            SegmentKeyword.objects.create(),
            SegmentVideo.objects.create()]
        urls = [
            reverse(
                "segment_api_urls:segment_list", kwargs={
                    "segment_type": segment.segment_type})
            for segment in segment_instances]
        for url in urls:
            response = self.client.get(url)
            self.assertTrue(
                "updated_at" in response.data.get("items")[0].keys())
