from datetime import datetime, date

from aw_reporting.models import AgeRanges, CampaignGenderTargeting, \
    CampaignAgeRangeTargeting, Opportunity, OpPlacement, Campaign, \
    GeoTarget, CampaignLocationTargeting
from aw_reporting.tools.health_check_tool import HealthCheckTool, MALE_GENDER, \
    FEMALE_GENDER, UNDETERMINED_GENDER, GENDERS
from utils.utils_tests import ExtendedAPITestCase as APITestCase

AGE_18_24 = "18-24"
AGE_25_34 = "25-34"


class SetupHealthCheckToolTestCase(APITestCase):
    def test_match_custom_affinity_targeting_1(self):
        opportunity = Opportunity.objects.create(
            id="1", name="", targeting_tactics="Custom Affinity")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="", salesforce_placement=placement,
            targeting_interests=True)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["targeting"]["match"], True)
        self.assertEqual(response["flight"]["aw"], [])

    def test_match_custom_affinity_targeting_2(self):
        opportunity = Opportunity.objects.create(
            id="1", name="", targeting_tactics="Custom Affinity")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="", salesforce_placement=placement,
            targeting_custom_affinity=True)
        Campaign.objects.create(
            id="2", name="", salesforce_placement=placement,
            targeting_interests=True, targeting_custom_affinity=True)
        Campaign.objects.create(
            id="3", name="", salesforce_placement=placement,
            targeting_interests=True)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertTrue(response.get("targeting").get("match"))
        for obj in response.get("targeting").get("aw"):
            self.assertTrue(obj.get("match"))
        for obj in response.get("targeting").get("sf"):
            self.assertTrue(obj.get("match"))

    def test_match_3_days_before_start(self):
        before_start = datetime(2017, 9, 22).date()
        start = datetime(2017, 9, 25).date()
        end = datetime(2017, 11, 26).date()
        opportunity = Opportunity.objects.create(
            id="1", name="", start=start, end=end)
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="", salesforce_placement=placement,
            start_date=before_start, end_date=end)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["flight"]["match"], True)

    def test_match_2_days_before_start(self):
        before_start = datetime(2017, 9, 23).date()
        start = datetime(2017, 9, 25).date()
        end = datetime(2017, 11, 26).date()
        opportunity = Opportunity.objects.create(
            id="1", name="", start=start, end=end)
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="", salesforce_placement=placement,
            start_date=before_start, end_date=end)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["flight"]["match"], True)

    def test_match_1_day_before_start(self):
        before_start = datetime(2017, 9, 24).date()
        start = datetime(2017, 9, 25).date()
        end = datetime(2017, 11, 26).date()
        opportunity = Opportunity.objects.create(
            id="1", name="", start=start, end=end)
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="", salesforce_placement=placement,
            start_date=before_start, end_date=end)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["flight"]["match"], True)

    def test_match_same_day_start(self):
        before_start = date(2017, 9, 25)
        start = date(2017, 9, 25)
        end = date(2017, 11, 26)
        opportunity = Opportunity.objects.create(
            id="1", name="", start=start, end=end)
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="", salesforce_placement=placement,
            start_date=before_start, end_date=end)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["flight"]["match"], True)

    def test_late_start(self):
        late_start = datetime(2017, 9, 26).date()
        start = datetime(2017, 9, 25).date()
        end = datetime(2017, 11, 26).date()
        opportunity = Opportunity.objects.create(
            id="1", name="", start=start, end=end)
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="", salesforce_placement=placement,
            start_date=late_start, end_date=end)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["flight"]["match"], False)

    def test_match_4_days_before_start(self):
        before_start = date(2017, 9, 26)
        start = date(2017, 9, 25)
        end = date(2017, 11, 26)
        opportunity = Opportunity.objects.create(
            id="1", name="", start=start, end=end)
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="", salesforce_placement=placement,
            start_date=before_start, end_date=end)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["flight"]["match"], False)

    def test_targeting_tactics_map(self):
        opportunity = Opportunity.objects.create(
            id="1", name="",
            targeting_tactics=
            "3p Targeting;Custom CAS;Influencer "
            "Audience Extension;Client 1st Party Data;"  # Re-marketing
            "Consumer Patterns / Life Events;"  # Interests
            "Viral and Trending Keywords",  # Keywords
        )
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="", salesforce_placement=placement,
            targeting_remarketings=True, targeting_interests=True,
            targeting_keywords=True)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["targeting"]["match"], True)
        self.assertEqual(set(e["name"] for e in response["targeting"]["sf"]),
                         set(opportunity.targeting_tactics.split(";")))
        self.assertEqual(set(e["name"] for e in response["targeting"]["aw"]),
                         {"Remarketing", "Interest", "Keyword"})

    def test_empty_fields(self):
        opportunity = Opportunity.objects.create(id="1", name="")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="", salesforce_placement=placement)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["demographic"]["match"], True)
        self.assertEqual(response["demographic"]["aw"], [])
        self.assertEqual(response["demographic"]["sf"], [])
        self.assertIs(response["targeting"]["match"], True)
        self.assertEqual(response["targeting"]["aw"], [])
        self.assertEqual(response["targeting"]["sf"], [])

    def test_short_name_geo_location_success(self):
        opportunity = Opportunity.objects.create(
            id="1", name="", geo_targeting="Cincinnati")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        campaign = Campaign.objects.create(
            id="1", name="", salesforce_placement=placement)
        geo_target, _ = GeoTarget.objects.get_or_create(
            id=1023626,
            defaults=dict(
                name="Cincinnati",
                canonical_name="Cincinnati,Ohio,United States",
            ))
        CampaignLocationTargeting.objects.create(
            campaign=campaign, location=geo_target)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["geographic"]["match"], True)
        self.assertEqual(response["geographic"]["aw"][0]["name"],
                         geo_target.canonical_name)
        self.assertEqual(response["geographic"]["sf"][0]["name"],
                         opportunity.geo_targeting)

    def test_name_with_state_name_after_comma_geo_location_success(self):
        opportunity = Opportunity.objects.create(
            id="1", name="",
            geo_targeting="DMAs:, Indianapolis, IN,"
                          " Columbus, OH, Cincinnati, OH")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        campaign = Campaign.objects.create(
            id="1", name="", salesforce_placement=placement)
        locations = (
            (1017146, "Indianapolis", "Indianapolis,Indiana,United States"),
            (1023640, "Columbus", "Columbus,Ohio,United States"),
            (1023626, "Cincinnati", "Cincinnati,Ohio,United States"),
        )
        for uid, name, c_name in locations:
            geo_target, _ = GeoTarget.objects.get_or_create(
                id=uid,
                defaults=dict(
                    name=name, canonical_name=c_name,
                ))
            CampaignLocationTargeting.objects.create(
                campaign=campaign, location=geo_target)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["geographic"]["match"], True)
        self.assertEqual(set(e["name"] for e in response["geographic"]["aw"]),
                         set(e[2] for e in locations))
        self.assertEqual(
            set(e["name"] for e in response["geographic"]["sf"]),
            {"DMAs:", "Indianapolis, IN", "Columbus, OH", "Cincinnati, OH"})

    def test_geo_location_sf_to_aw_map(self):
        opportunity = Opportunity.objects.create(
            id="1", name="", geo_targeting="USA Only")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        campaign = Campaign.objects.create(
            id="1", name="", salesforce_placement=placement)
        geo_target, _ = GeoTarget.objects.get_or_create(
            id=2840,
            defaults=dict(
                name="United States",
                canonical_name="United States",
            ))
        CampaignLocationTargeting.objects.create(
            campaign=campaign, location=geo_target)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["geographic"]["match"], True)
        self.assertEqual(response["geographic"]["aw"][0]["name"],
                         geo_target.canonical_name)
        self.assertEqual(response["geographic"]["sf"][0]["name"],
                         opportunity.geo_targeting)

    def test_geo_locations_comma_separator(self):
        opportunity = Opportunity.objects.create(
            id="1", name="",
            geo_targeting="New York, Washington DC, Boston,"
                          " Buffalo, Syracuse, Albany NY,"
                          " Rochester, Baltimore")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        campaign = Campaign.objects.create(
            id="1", name="", salesforce_placement=placement)
        locations = (
            (1023191, "New York", "New York,New York,United States"),
            (1027045, "Boston", "Boston,Virginia,United States"),
            (1027327, "Washington", "Washington,Virginia,United States"),
            (1018511, "Baltimore", "Baltimore,Maryland,United States"),
            (1019742, "Buffalo", "Buffalo,Minnesota,United States"),
            (1020163, "Albany", "Albany,Missouri,United States"),
            (1025242, "Rochester", "Rochester,Pennsylvania,United States"),
            (1027001, "Syracuse", "Syracuse,Utah,United States"))
        for uid, name, c_name in locations:
            geo_target, _ = GeoTarget.objects.get_or_create(
                id=uid,
                defaults=dict(
                    name=name, canonical_name=c_name,
                ))
            CampaignLocationTargeting.objects.create(
                campaign=campaign, location=geo_target)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["geographic"]["match"], True)
        self.assertEqual(set(e["name"] for e in response["geographic"]["aw"]),
                         set(e[2] for e in locations))
        self.assertEqual(
            set(e["name"] for e in response["geographic"]["sf"]),
            set(e.strip() for e in opportunity.geo_targeting.split(",")))

    def test_geo_locations_and_numbers_to_spend(self):
        opportunity = Opportunity.objects.create(
            id="1", name="",
            geo_targeting="Peru 40%, Chile 60%")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        campaign = Campaign.objects.create(
            id="1", name="", salesforce_placement=placement)
        locations = (
            (2152, "Chile", "Chile"),
            (2604, "Peru", "Peru"))
        for uid, name, c_name in locations:
            geo_target, _ = GeoTarget.objects.get_or_create(
                id=uid,
                defaults=dict(
                    name=name, canonical_name=c_name,
                ))
            CampaignLocationTargeting.objects.create(
                campaign=campaign, location=geo_target)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["geographic"]["match"], True)
        self.assertEqual(set(e["name"] for e in response["geographic"]["aw"]),
                         set(e[2] for e in locations))
        self.assertEqual(
            set(e["name"] for e in response["geographic"]["sf"]),
            set(e.strip() for e in opportunity.geo_targeting.split(",")))

    def test_tag_section_success_1(self):
        opportunity = Opportunity.objects.create(
            id="1", name="", tags="Attached See Below")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="", salesforce_placement=placement,
            tracking_template_is_set=True)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["tags"]["match"], True)
        self.assertEqual(response["tags"]["sf"][0]["name"], "Yes")
        self.assertEqual(response["tags"]["aw"][0]["name"], "Yes")

    def test_tag_section_success_2(self):
        opportunity = Opportunity.objects.create(id="1", name="", tags="")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="",
            salesforce_placement=placement, tracking_template_is_set=False)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["tags"]["match"], True)
        self.assertEqual(response["tags"]["sf"][0]["name"], "No")
        self.assertEqual(response["tags"]["aw"][0]["name"], "No")

    def test_tag_section_success_sql_segmentation(self):
        opportunity = Opportunity.objects.create(
            id="1", name="", tags="Attached See Below")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        campaign = Campaign.objects.create(
            id="1", name="", salesforce_placement=placement,
            tracking_template_is_set=True)
        CampaignGenderTargeting.objects.create(campaign=campaign, gender_id=0)
        CampaignGenderTargeting.objects.create(campaign=campaign, gender_id=1)
        CampaignGenderTargeting.objects.create(campaign=campaign, gender_id=2)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["tags"]["match"], True)
        self.assertEqual(response["tags"]["sf"][0]["name"], "Yes")
        self.assertEqual(response["tags"]["aw"][0]["name"], "Yes")

    def test_tag_section_fail_1(self):
        opportunity = Opportunity.objects.create(
            id="1", name="", tags="Attached See Below")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="", salesforce_placement=placement,
            tracking_template_is_set=False)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["tags"]["match"], False)
        self.assertEqual(response["tags"]["sf"][0]["name"], "Yes")
        self.assertEqual(response["tags"]["aw"][0]["name"], "No")

    def test_tag_section_fail_2(self):
        opportunity = Opportunity.objects.create(
            id="1", name="", tags="Attached See Below")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="", salesforce_placement=placement,
            tracking_template_is_set=False)
        Campaign.objects.create(
            id="2", name="", salesforce_placement=placement,
            tracking_template_is_set=True)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["tags"]["match"], False)
        self.assertEqual(response["tags"]["sf"][0]["name"], "Yes")
        self.assertEqual(response["tags"]["aw"][0]["name"], "1/2")

    def test_tag_section_fail_3(self):
        opportunity = Opportunity.objects.create(id="1", name="", tags="")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="",
            salesforce_placement=placement, tracking_template_is_set=True)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["tags"]["match"], False)
        self.assertEqual(response["tags"]["sf"][0]["name"], "No")
        self.assertEqual(response["tags"]["aw"][0]["name"], "Yes")

    def test_tag_section_fail_4(self):
        opportunity = Opportunity.objects.create(
            id="1", name="", tags="Attached See Below")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="",
            salesforce_placement=placement, tracking_template_is_set=False)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["tags"]["match"], False)
        self.assertEqual(response["tags"]["sf"][0]["name"], "Yes")
        self.assertEqual(response["tags"]["aw"][0]["name"], "No")

    def test_demo_section(self):
        opportunity = Opportunity.objects.create(
            id="1", name="", demographic="A 18 - 24; A 25 - 34;"
                                         "No unknown age;Unknown Gender")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        campaign = Campaign.objects.create(
            id="1", name="", salesforce_placement=placement)
        CampaignGenderTargeting.objects.create(
            campaign=campaign, gender_id=GENDERS.index(UNDETERMINED_GENDER))
        CampaignGenderTargeting.objects.create(
            campaign=campaign, gender_id=GENDERS.index(FEMALE_GENDER))
        CampaignGenderTargeting.objects.create(
            campaign=campaign, gender_id=GENDERS.index(MALE_GENDER))
        CampaignAgeRangeTargeting.objects.create(
            campaign=campaign, age_range_id=AgeRanges.index(AGE_18_24))
        CampaignAgeRangeTargeting.objects.create(
            campaign=campaign, age_range_id=AgeRanges.index(AGE_25_34))
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["demographic"]["match"], True)
        self.assertEqual(
            set(e["name"] for e in response["demographic"]["aw"]),
            {UNDETERMINED_GENDER, FEMALE_GENDER,
             MALE_GENDER, AGE_18_24, AGE_25_34})
        self.assertEqual(
            set(e["name"] for e in response["demographic"]["sf"]),
            set(e.strip() for e in opportunity.demographic.split(";")))

    def test_gdn_section_success_1(self):
        opportunity = Opportunity.objects.create(
            id="1", name="", types_of_targeting="GDN")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="", salesforce_placement=placement,
            targeting_excluded_channels=True,
            targeting_excluded_topics=True,
            targeting_excluded_keywords=True)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["gdn"]["match"], True)
        self.assertIs(response["gdn"]["sf"][0]["name"], "Yes")
        self.assertIs(response["gdn"]["aw"][0]["name"], "Yes")

    def test_gdn_section_success_2(self):
        opportunity = Opportunity.objects.create(
            id="1", name="", types_of_targeting="")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="", salesforce_placement=placement,
            targeting_excluded_channels=False,
            targeting_excluded_topics=False,
            targeting_excluded_keywords=False)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["gdn"]["match"], True)
        self.assertIs(response["gdn"]["sf"][0]["name"], "No")
        self.assertIs(response["gdn"]["aw"][0]["name"], "No")

    def test_gdn_section_success_3(self):
        opportunity = Opportunity.objects.create(
            id="1", name="", types_of_targeting="GDN")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="", salesforce_placement=placement,
            targeting_excluded_channels=True,
            targeting_excluded_topics=True,
            targeting_excluded_keywords=True)
        Campaign.objects.create(
            id="2", name="", salesforce_placement=placement,
            targeting_excluded_channels=True,
            targeting_excluded_topics=True,
            targeting_excluded_keywords=True)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["gdn"]["match"], True)
        self.assertIs(response["gdn"]["sf"][0]["name"], "Yes")
        self.assertIs(response["gdn"]["aw"][0]["name"], "Yes")

    def test_gdn_section_fail_1(self):
        opportunity = Opportunity.objects.create(
            id="1", name="", types_of_targeting="GDN")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="", salesforce_placement=placement,
            targeting_excluded_channels=True,
            targeting_excluded_topics=True,
            targeting_excluded_keywords=True)
        Campaign.objects.create(
            id="2", name="", salesforce_placement=placement,
            targeting_excluded_channels=True,
            targeting_excluded_topics=False,
            targeting_excluded_keywords=True)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["gdn"]["match"], False)
        self.assertIs(response["gdn"]["sf"][0]["name"], "Yes")
        self.assertIs(response["gdn"]["aw"][0]["name"], "No")

    def test_gdn_section_fail_2(self):
        opportunity = Opportunity.objects.create(
            id="1", name="", types_of_targeting="GDN")
        OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["gdn"]["match"], False)
        self.assertIs(response["gdn"]["sf"][0]["name"], "Yes")
        self.assertIs(response["gdn"]["aw"][0]["name"], "No")

    def test_gdn_section_fail_3(self):
        opportunity = Opportunity.objects.create(
            id="1", name="", types_of_targeting="")
        placement = OpPlacement.objects.create(
            id="1", name="", opportunity=opportunity)
        Campaign.objects.create(
            id="1", name="", salesforce_placement=placement,
            targeting_excluded_channels=True,
            targeting_excluded_topics=True,
            targeting_excluded_keywords=True)
        result = HealthCheckTool([opportunity])
        self.assertEqual(len(result), 1)
        response = result[0]
        self.assertIs(response["gdn"]["match"], False)
        self.assertIs(response["gdn"]["sf"][0]["name"], "No")
        self.assertIs(response["gdn"]["aw"][0]["name"], "Yes")
