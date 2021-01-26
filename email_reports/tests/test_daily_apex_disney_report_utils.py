import enum
from mock import patch

from django.test.testcases import TestCase
from email_reports.reports.daily_apex_disney_campaign_report import TrackingUrlTemplateDisneyIdParser
from email_reports.reports.daily_apex_disney_campaign_report import DisneyTagSheetMap


def init_file(self):
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
