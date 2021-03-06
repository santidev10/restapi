from mock import patch

from django.test.testcases import TestCase
from es_components.constants import Sections
from es_components.managers.video import VideoManager
from es_components.models.video import Video

from email_reports.models import VideoCreativeData
from email_reports.reports.daily_apex_disney_campaign_report import DisneyTagSheetMap
from email_reports.reports.daily_apex_disney_campaign_report import DisneyTrackingUrlTemplateIdParser
from email_reports.reports.daily_apex_visa_campaign_report import ApexVisaCreativeDataAggregator
from email_reports.reports.daily_apex_visa_campaign_report import TITLE


def init_file(self):
    """
    sets the DisneyTagSheetMap's reader as an iterable (simulates a csv of our choosing)
    :param self:
    :return:
    """
    lines = [
        ["header", "row"],
        ["ad_id_1", "ad_name_1", "camp_id_1", "camp_name_1", "pl_id_1", "", "", "pl_name_1", "", "", "", "", "", "",
         "cr_id_1", "cr_name_1"],
        ["ad_id_2", "ad_name_2", "camp_id_2", "camp_name_2", "pl_id_2", "", "", "pl_name_2", "", "", "", "", "", "",
         "cr_id_2", "cr_name_2"],
        ["ad_id_3", "ad_name_3", "camp_id_3", "camp_name_3", "pl_id_3", "", "", "pl_name_3", "", "", "", "", "", "",
         "cr_id_3", "cr_name_3"],
        # only one link to advertiser
        ["ad_id_4", "ad_name_4", "", "", "pl_id_4", "", "", "pl_name_4", "", "", "", "", "", "", "", ""],
        ["ad_id_4", "ad_name_4", "camp_id_4", "camp_name_4", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["ad_id_4", "ad_name_4", "", "", "", "", "", "", "", "", "", "", "", "", "cr_id_4", "cr_name_4"],
    ]
    self.reader = iter(lines)


def do_nothing(*args, **kwargs):
    pass


def get_mock_es_videos_from_data(data: dict):
    videos = []
    for id, video_data in data.items():
        video = Video(id=id)
        general_data = video_data.get(Sections.GENERAL_DATA, {})
        video.populate_general_data(**general_data)
        d = video.to_dict()
    videos.append(video)
    return videos


class ApexVisaCreativeDataAggregatorTestCase(TestCase):

    mock_youtube_data = {
        'afuMSgwprO4': {'general_data': {'title': 'Where You Shop Matters - Bear Market Coffee',
                                         'duration': 10.0,
                                         'thumbnail_image_url': 'https://i.ytimg.com/vi/afuMSgwprO4/default.jpg'}},
        'vYpKowDmrdE': {'general_data': {'title': 'Alle Ausgaben im Blick. Ich zahle Visa',
                                         'duration': 6.0,
                                         'thumbnail_image_url': 'https://i.ytimg.com/vi/vYpKowDmrdE/default.jpg'}},
        '754rLvM9Yfw': {'general_data': {'title': 'Pre Roll 10sec Final 02',
                                         'duration': 10.0,
                                         'thumbnail_image_url': 'https://i.ytimg.com/vi/754rLvM9Yfw/default.jpg'}},
        'OLdQe4bcizk': {
            'general_data': {'title': 'Mit gutem Gef??hl online einkaufen. Ich zahle Visa',
                             'duration': 6.0,
                             'thumbnail_image_url': 'https://i.ytimg.com/vi/OLdQe4bcizk/default.jpg'}},
        'MMzFjTln1Hg': {'general_data': {'title': 'Alle Ausgaben im Blick. Ich zahle Visa',
                                         'duration': 20.0,
                                         'thumbnail_image_url': 'https://i.ytimg.com/vi/MMzFjTln1Hg/default.jpg'}},
    }

    mock_es_data = {
        'fTjuOpKziWs': {
            'general_data': {'title': 'Entdecke alle Bezahlm??glichkeiten, die du mit Visa hast',
                             'duration': 6.0,
                             'thumbnail_image_url': 'https://i.ytimg.com/vi/fTjuOpKziWs/default.jpg'}},
        '7kXDIx-V6GE': {
            'general_data': {'title': 'Flexibel bleiben, was auch passiert. Ich zahle Visa',
                             'duration': 20.0,
                             'thumbnail_image_url': 'https://i.ytimg.com/vi/7kXDIx-V6GE/default.jpg'}},
        '8jjZCjWo-rM': {'general_data': {'title': 'Pre Roll 15sec Final 02',
                                         'duration': 15.0,
                                         'thumbnail_image_url': 'https://i.ytimg.com/vi/8jjZCjWo-rM/default.jpg'}},
        '8yRahHclzUw': {
            'general_data': {'title': 'Mit gutem Gef??hl online einkaufen. Ich zahle Visa',
                             'duration': 20.0,
                             'thumbnail_image_url': 'https://i.ytimg.com/vi/8yRahHclzUw/default.jpg'}},
        '_JFJOPmDPvQ': {
            'general_data': {'title': 'Flexibel bleiben, was auch passiert. Ich zahle Visa',
                             'duration': 6.0,
                             'thumbnail_image_url': 'https://i.ytimg.com/vi/_JFJOPmDPvQ/default.jpg'}}
    }

    def test_data_persisted_successfully(self):
        """
        make sure we're pulling and storing (mocked) data from es and youtube correctly
        :return:
        """
        with patch("utils.youtube_api.resolve_videos_info", return_value=self.mock_youtube_data), \
                patch.object(VideoManager, "get", return_value=get_mock_es_videos_from_data(self.mock_es_data)):
            creatives_data = {**self.mock_youtube_data, **self.mock_es_data}
            creative_ids = creatives_data.keys()
            aggregator = ApexVisaCreativeDataAggregator(creative_ids=creative_ids)
            query = VideoCreativeData.objects.filter(id__in=creative_ids)
            stored_ids = list(query.values_list("id", flat=True))
            self.assertEqual(set(creative_ids), set(stored_ids))
            for id in creative_ids:
                with self.subTest(id):
                    agg_creative_data = aggregator.get(id, default={})
                    mock_creative_data = creatives_data.get(id, {})
                    self.assertEqual(agg_creative_data.get(Sections.GENERAL_DATA, {}).get(TITLE),
                                     mock_creative_data.get(Sections.GENERAL_DATA, {}).get(TITLE))

            for video_data in query:
                with self.subTest(video_data.id):
                    self.assertTrue(video_data.data.get(Sections.GENERAL_DATA, {}).get(TITLE))

    def test_data_validation(self):
        self.assertTrue(ApexVisaCreativeDataAggregator._data_is_valid({Sections.GENERAL_DATA: {TITLE: "title"}}))
        self.assertFalse(ApexVisaCreativeDataAggregator._data_is_valid({Sections.IAS_DATA: {TITLE: "title"}}))


class DisneyTagSheetMapTestCase(TestCase):

    @patch.object(DisneyTagSheetMap, "_init_file", init_file)
    def test_init_success(self):
        tag_sheet_map = DisneyTagSheetMap()
        # ensure maps exist
        for attr in ["campaign_map", "placement_map", "creative_map", "campaign_advertiser_map",
                     "placement_advertiser_map", "creative_advertiser_map"]:
            with self.subTest(attr):
                self.assertTrue(hasattr(tag_sheet_map, attr))
        # ensure correct number of items are mapped
        for map_name in ["campaign_map", "placement_map", "creative_map"]:
            with self.subTest(map_name):
                map_item = getattr(tag_sheet_map, map_name)
                self.assertEqual(len(map_item), 4)
                self.assertNotIn("", map_item.keys())
                self.assertNotIn("", map_item.values())

    @patch.object(DisneyTagSheetMap, "_init_file", init_file)
    def test_mapping_success(self):
        tag_sheet_map = DisneyTagSheetMap()
        # ensure mapper class gets the right name for a given id
        for id, name, method_name in [("camp_id_1", "camp_name_1", "get_campaign_name"),
                                      ("pl_id_1", "pl_name_1", "get_placement_name"),
                                      ("cr_id_1", "cr_name_1", "get_creative_name")]:
            with self.subTest(id):
                method = getattr(tag_sheet_map, method_name)
                self.assertEqual(method(id), name)
        # ensure that only one existing campaign/placement/creative id is required to map an advertiser id and name
        for args in [("camp_id_4", None, None),
                     (None, "pl_id_4", None),
                     (None, None, "cr_id_4")]:
            with self.subTest(args):
                self.assertEqual(tag_sheet_map.get_advertiser_id_and_name(*args), ("ad_id_4", "ad_name_4"))

    @patch.object(DisneyTagSheetMap, "_init_file", do_nothing)
    def test_initializes_with_empty_maps_if_no_csv(self):
        """
        if, csv is not present, the report should still send
        :return:
        """
        tag_sheet_map = DisneyTagSheetMap()
        for map_name in ["campaign_map", "placement_map", "creative_map", "campaign_advertiser_map",
                         "placement_advertiser_map", "creative_advertiser_map"]:
            with self.subTest(map_name):
                self.assertTrue(hasattr(tag_sheet_map, map_name))
                self.assertEqual(len(getattr(tag_sheet_map, map_name)), 0)


class DisneyTrackingUrlTemplateIdParserTestCase(TestCase):
    valid_url = ("https://ad.doubleclick.net/ddm/trackclk/N1032334.3219362APEXDEALS/B24438688.278422249;dc_trk_aid=4725"
                 "17684;dc_trk_cid=135340449;dc_lat=;dc_rdid=;tag_for_child_directed_treatment=;tfua=")
    partial_campaign_placement = ("https://ad.doubleclick.net/ddm/trackclk/N1032334.3219362APEXDEALS/B24438688.27842224"
                                  "9;dc_trk_aid=472517684;dc_lat=;dc_rdid=;tag_for_child_directed_treatment=;tfua=")
    partial_creative = ("https://ad.doubleclick.net/ddm/trackclk/N1032334.3219362APEXDEALS;dc_trk_aid=472517684;dc_trk_"
                        "cid=135340449;dc_lat=;dc_rdid=;tag_for_child_directed_treatment=;tfua=")
    wrong_domain = ("https://www.not-doubleclick.net/ddm/trackclk/N1032334.3219362APEXDEALS/B24438688.278422249;dc_trk_"
                    "aid=472517684;dc_trk_cid=135340449;dc_lat=;dc_rdid=;tag_for_child_directed_treatment=;tfua=")
    campaign_id_not_int = "https://ad.doubleclick.net/BnotAnInt.1234"

    def test_campaign_path_parts_computed_once(self):
        """
        campaign_placement_path_part should only be parsed once
        :return:
        """
        parser = DisneyTrackingUrlTemplateIdParser(self.valid_url)
        parser.campaign_placement_path_part = replacement_parts = "asdf"
        parts = parser._get_campaign_placement_path_part()
        self.assertEqual(parts, replacement_parts)

    def test_campaign_path_parts_valid_url_success(self):
        """
        test that pulling the raw campaign/placement path part is successful
        :return:
        """
        parser = DisneyTrackingUrlTemplateIdParser(self.valid_url)
        self.assertEqual(parser._get_campaign_placement_path_part(), "B24438688.278422249")

    def test_get_campaign_id_success(self):
        """
        test that campaign id is parsed correctly
        :return:
        """
        parser = DisneyTrackingUrlTemplateIdParser(self.valid_url)
        self.assertEqual(parser.get_campaign_id(), "24438688")

    def test_get_placement_id_success(self):
        """
        test that placement id is parsed correctly
        :return:
        """
        parser = DisneyTrackingUrlTemplateIdParser(self.valid_url)
        self.assertEqual(parser.get_placement_id(), "278422249")

    def test_get_creative_id_success(self):
        """
        test that creative id is parsed correctly
        :return:
        """
        parser = DisneyTrackingUrlTemplateIdParser(self.valid_url)
        self.assertEqual(parser.get_creative_id(), "135340449")

    def test_partial_parse_sans_creative_success(self):
        """
        missing creative id should not affect campaign/placement
        :return:
        """
        parser = DisneyTrackingUrlTemplateIdParser(self.partial_campaign_placement)
        self.assertEqual(parser.get_campaign_id(), "24438688")
        self.assertEqual(parser.get_placement_id(), "278422249")
        self.assertEqual(parser.get_creative_id(), None)

    def test_partial_parse_sans_campaign_placement_success(self):
        """
        missing campaign/placement id should not affect creative
        :return:
        """
        parser = DisneyTrackingUrlTemplateIdParser(self.partial_creative)
        self.assertEqual(parser.get_campaign_id(), None)
        self.assertEqual(parser.get_placement_id(), None)
        self.assertEqual(parser.get_creative_id(), "135340449")

    def test_get_campaign_id_returns_none_when_not_an_integer(self):
        """
        campaign id should be an integer, or None
        :return:
        """
        parser = DisneyTrackingUrlTemplateIdParser(self.campaign_id_not_int)
        self.assertEqual(parser.get_campaign_id(), None)

    def test_returns_none_when_invalid_domain(self):
        """
        domain must be ad.doubleclick.net
        :return:
        """
        parser = DisneyTrackingUrlTemplateIdParser(self.wrong_domain)
        self.assertEqual(parser.get_campaign_id(), None)
        self.assertEqual(parser.get_placement_id(), None)
        self.assertEqual(parser.get_creative_id(), None)
