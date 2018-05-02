import re
from collections import defaultdict
from itertools import zip_longest

from django.contrib.auth import get_user_model
from django.db.models import Min, Max, Count, Case, When, F, Sum, CharField

from aw_reporting.models import Campaign, ConcatAggregate, GeoTarget, \
    VideoCreativeStatistic

AW_TARGETING_FIELDS = (
    "targeting_interests", "targeting_topics", "targeting_keywords",
    "targeting_remarketings", "targeting_channels", "targeting_videos",
    "targeting_custom_affinity", "tracking_template_is_set",
    "targeting_excluded_channels", "targeting_excluded_topics",
    "targeting_excluded_keywords")

AGE_RANGES = ("Undetermined Age", "18-24", "25-34", "35-44", "45-54", "55-64",
              "65 or more")

UNDETERMINED_GENDER = "Undetermined Gender"
MALE_GENDER = "Male"
FEMALE_GENDER = "Female"
GENDERS = (UNDETERMINED_GENDER, FEMALE_GENDER, MALE_GENDER)


class HealthCheckTool(list):
    user_fields = (
        "ad_ops_manager", "ad_ops_qa_manager",
        "account_manager", "sales_manager")

    def __init__(self, items):
        opp_ids = [o.id for o in items]
        emails = [
            getattr(getattr(o, f), "email", None)
            for o in items for f in self.user_fields]
        emails = set(filter(None, emails))
        user_thumbnails = get_user_model().objects.filter(
            email__in=emails).values("email", "profile_image_url")
        self.user_thumbnails = {
            u["email"]: u["profile_image_url"] for u in user_thumbnails}
        group_by = "salesforce_placement__opportunity_id"
        aw_setup_data = Campaign.objects.filter(
            salesforce_placement__opportunity_id__in=opp_ids
        ).values(group_by).order_by(group_by).annotate(
            **self.get_aw_annotate())
        aw_setup_data = {e[group_by]: e for e in aw_setup_data}
        # thumbnails
        key = "ad_group__campaign__salesforce_placement__opportunity_id"
        thumb_data = VideoCreativeStatistic.objects.filter(
            **{"{}__in".format(key): opp_ids}
        ).values(key, "creative_id").order_by(
            key, "creative_id").annotate(sum_cost=Sum("cost"))
        thumbnail_lists = defaultdict(list)
        for e in thumb_data:
            thumbnail_lists[e[key]].append((e["sum_cost"], e["creative_id"]))
        thumbnails = {}
        for k, v in thumbnail_lists.items():
            if v:
                yt_id = list(sorted(v, reverse=True))[0][1]
                thumbnails[k] = "https://i.ytimg.com/vi/{}/" \
                                "hqdefault.jpg".format(yt_id)
        result = []
        for obj in items:
            aw_data = aw_setup_data.get(obj.id)
            item = dict(
                id=obj.id, name=obj.name, thumbnail=thumbnails.get(obj.id),
                brand=obj.brand,
                qa=self.format_user_field(obj.ad_ops_qa_manager),
                am=self.format_user_field(obj.account_manager),
                ad_ops=self.format_user_field(obj.ad_ops_manager),
                sales=self.format_user_field(obj.sales_manager),
                demographic=DemoSection(obj, aw_data),
                geographic=GeoSection(obj, aw_data),
                flight=FlightSection(obj, aw_data),
                targeting=TargetingSection(obj, aw_data),
                tags=TagsSection(obj, aw_data),
                gdn=GDNSection(obj, aw_data),
            )
            result.append(item)
        super(HealthCheckTool, self).__init__(result)

    def format_user_field(self, obj):
        if obj is None:
            return None
        thumbnail = self.user_thumbnails.get(obj.email)
        r = dict(id=obj.id, name=obj.name, thumbnail=thumbnail)
        return r

    @staticmethod
    def get_aw_annotate():
        annotate = dict(
            gender_targeting=ConcatAggregate(
                "gender_targeting__gender_id", distinct=True),
            age_range_targeting=ConcatAggregate(
                "age_range_targeting__age_range_id", distinct=True),
            location_targeting=ConcatAggregate(
                "location_targeting__location_id", distinct=True),
            min_start=Min("start_date"), max_end=Max("end_date"),
            end_null_count=Count(
                Case(
                    When(
                        end_date__isnull=True,
                        then=F('id'),
                    ),
                    output_field=CharField()
                ),
                distinct=True,
            ),
            count=Count("id", distinct=True),
        )
        for f in AW_TARGETING_FIELDS:
            annotate[f] = Count(
                Case(
                    When(
                        then=F('id'),
                        **{f: True}
                    ),
                    output_field=CharField()
                ),
                distinct=True,
            )
        return annotate


class Section(dict):
    positive = "Yes"
    negative = "No"
    coma_sep = ", "
    semicolon_sep = ";"

    def __init__(self, obj, setup_data):
        aw = [self.objectify(o) for o in self.prepare_aw_field(setup_data)]
        sf = [self.objectify(o) for o in self.prepare_sf_field(obj)]
        aw, sf = self.run_comparison(aw, sf)
        match = self.match(aw, sf)
        super(Section, self).__init__(aw=aw, sf=sf, match=match)

    @staticmethod
    def objectify(item_name):
        return dict(name=item_name, match=False)

    @staticmethod
    def set_positive_matching(el):
        el["match"] = True

    def prepare_aw_field(self, setup_data):
        raise NotImplementedError

    def prepare_sf_field(self, obj):
        raise NotImplementedError

    def run_comparison(self, aw, sf):
        for sf_item in sf:
            for aw_item in aw:
                if aw_item["name"] == sf_item["name"]:
                    self.set_positive_matching(aw_item)
                    self.set_positive_matching(sf_item)
                    continue
        return aw, sf

    @staticmethod
    def match(aw, sf):
        return all(o["match"] for o in aw) and all(o["match"] for o in sf)


class DemoSection(Section):
    aw_undetermined_age = "Undetermined Age"
    aw_undetermined_gender = "Undetermined Gender"
    aw_male = "Male"
    aw_female = "Female"

    sf_any_gender = "A "
    sf_male = "M"
    sf_female = "F"
    sf_unknown = "Unknown"
    sf_no_unknown_age = "No unknown age"
    sf_no_unknown_gender = "No unknown gender"
    sf_no_unknown_age_gender = "No unknown age/gender"
    sf_unknown_age = "Unknown Age"
    sf_unknown_gender = "Unknown Gender"

    def __init__(self, *args, **kwargs):
        self.gender_targeting = []
        self.age_range_targeting = []
        super(DemoSection, self).__init__(*args, **kwargs)

    def prepare_aw_field(self, setup_data):
        if not setup_data:
            return []
        response = []
        gender_targeting = setup_data["gender_targeting"]
        if gender_targeting:
            gender_targeting = [
                GENDERS[int(e)] for e in
                gender_targeting.split(self.coma_sep)]
            self.gender_targeting = gender_targeting
            response.extend(gender_targeting)

        age_range_targeting = setup_data["age_range_targeting"]
        if age_range_targeting:
            age_range_targeting = [
                AGE_RANGES[int(e)] for e in
                age_range_targeting.split(self.coma_sep)]
            self.age_range_targeting = age_range_targeting
            response.extend(age_range_targeting)
        return response

    def prepare_sf_field(self, obj):
        goal = (e.strip() for e in obj.demographic.split(self.semicolon_sep))
        goal = list(filter(None, goal))
        return goal

    def run_comparison(self, aw, sf):
        sf = [
            {"match_age": False,
             "match_gender": False,
             "match": False,
             "name": obj["name"]} for obj in sf]
        string_aw_names = " ".join([obj["name"] for obj in aw])
        string_sf_names = " ".join([obj["name"] for obj in sf]).replace(
            " - ", "-")
        # Any gender from SF
        if self.sf_any_gender in string_sf_names:
            if self.aw_male in string_aw_names:
                for obj in aw:
                    if obj["name"] == self.aw_male:
                        obj["match"] = True
                        break
            if self.aw_female in string_aw_names:
                for obj in aw:
                    if obj["name"] == self.aw_female:
                        obj["match"] = True
                        break
            if self.aw_male in string_aw_names or \
                    self.aw_female in string_aw_names:
                for obj in sf:
                    if obj["name"].startswith(self.sf_any_gender):
                        obj["match_gender"] = True
        # Female gender from SF
        if self.sf_female in string_sf_names:
            if self.aw_female in string_aw_names:
                for obj in aw:
                    if obj["name"] == self.aw_female:
                        obj["match"] = True
                        break
                for obj in sf:
                    if obj["name"].startswith(self.sf_female):
                        obj["match_gender"] = True
        # Male gender from SF
        if self.sf_male in string_sf_names:
            if self.aw_male in string_aw_names:
                for obj in aw:
                    if obj["name"] == self.aw_male:
                        obj["match"] = True
                        break
                for obj in sf:
                    if obj["name"].startswith(self.sf_male):
                        obj["match_gender"] = True
        # Unknown from SF
        if self.sf_unknown in string_sf_names:
            if self.aw_undetermined_gender in string_aw_names:
                for obj in aw:
                    if obj["name"] == self.aw_undetermined_gender:
                        obj["match"] = True
                        break
            if self.aw_undetermined_age in string_aw_names:
                for obj in aw:
                    if obj["name"] == self.aw_undetermined_age:
                        obj["match"] = True
                        break
            if self.aw_undetermined_gender in string_aw_names or \
                    self.aw_undetermined_age in string_aw_names:
                for obj in sf:
                    if obj["name"] == self.sf_unknown:
                        obj["match"] = True
                        break
        # Unknown Age from SF
        if self.sf_unknown_age in string_sf_names:
            if self.aw_undetermined_age in string_aw_names:
                for obj in aw:
                    if obj["name"] == self.aw_undetermined_age:
                        obj["match"] = True
                        break
                for obj in sf:
                    if obj["name"] == self.sf_unknown_age:
                        obj["match"] = True
                        break
        # Unknown Gender from SF
        if self.sf_unknown_gender in string_sf_names:
            if self.aw_undetermined_gender in string_aw_names:
                for obj in aw:
                    if obj["name"] == self.aw_undetermined_gender:
                        obj["match"] = True
                        break
                for obj in sf:
                    if obj["name"] == self.sf_unknown_gender:
                        obj["match"] = True
                        break
        # No unknown Age from SF
        if self.sf_no_unknown_age in string_sf_names:
            if self.aw_undetermined_age not in string_aw_names:
                for obj in sf:
                    if obj["name"] == self.sf_no_unknown_age:
                        obj["match"] = True
                        break
        # No unknown Gender from SF
        if self.sf_no_unknown_gender in string_sf_names:
            if self.aw_undetermined_gender not in string_aw_names:
                for obj in sf:
                    if obj["name"] == self.sf_no_unknown_gender:
                        obj["match"] = True
                        break
        # No unknown Age or Gender from SF
        if self.sf_no_unknown_age_gender in string_sf_names:
            if self.aw_undetermined_age not in string_aw_names and \
                    self.aw_undetermined_gender not in string_aw_names:
                for obj in sf:
                    if obj["name"] == self.sf_no_unknown_age_gender:
                        obj["match"] = True
                        break
        # Age ranges
        fill_value = 0
        aw_age_ranges = self.make_pairs(
            re.findall(r"\d+", string_aw_names), fill_value=fill_value)
        sf_age_ranges = self.make_pairs(
            re.findall(r"\d+", string_sf_names), fill_value=fill_value)
        for pair in sf_age_ranges:
            if pair in aw_age_ranges and pair[1] != fill_value:
                for obj in aw:
                    if obj["name"] == "-".join(pair):
                        obj["match"] = True
                        break
                for obj in sf:
                    if " - ".join(pair) in obj["name"]:
                        obj["match_age"] = True
            else:
                value = pair[0]
                for obj in aw:
                    if value in obj["name"]:
                        obj["match"] = True
                        break
                for obj in sf:
                    if value in obj["name"]:
                        obj["match_age"] = True
                        break
        # Finalize
        for obj in sf:
            if obj["match"] is False:
                if all((obj["match_age"], obj["match_gender"])):
                    obj["match"] = True
            del obj["match_age"]
            del obj["match_gender"]
        return aw, sf

    @staticmethod
    def make_pairs(sequence, size=2, fill_value=0):
        if not sequence:
            return tuple()
        obj = iter(sequence)
        return tuple(zip_longest(*[obj] * size, fillvalue=fill_value))


class GeoSection(Section):
    UNITED_STATES = "United States"
    SF_TO_AW_MAP = {
        "National": UNITED_STATES,
        "US National": UNITED_STATES,
        "USA National": UNITED_STATES,
        "US Only": UNITED_STATES,
        "USA Only": UNITED_STATES,
        "US": UNITED_STATES,
        "USA": UNITED_STATES,
    }

    def prepare_aw_field(self, setup_data):
        if setup_data and setup_data["location_targeting"]:
            location_targeting = [
                int(e) for e in setup_data[
                    "location_targeting"].split(self.coma_sep)]
            location_targeting = list(
                GeoTarget.objects.filter(
                    id__in=location_targeting).values_list(
                    "canonical_name", flat=True))
            return location_targeting
        return []

    def prepare_sf_field(self, obj):
        """
        DMAs:, Indianapolis, IN, Columbus, OH, Cincinnati, OH
        to
        ["DMAs:", "Indianapolis, IN", "Columbus, OH", "Cincinnati, OH"]
        :param obj:
        :return:
        """
        items = []
        pattern = re.compile(r",(?! [A-Z]{2})")
        for line in obj.geo_targeting.split("\n"):
            for word in pattern.split(line):
                word = word.strip()
                if word:
                    items.append(word)
        return items

    def run_comparison(self, aw, sf):
        aw_by_names = {o["name"]: o for o in aw}
        aw_by_short_names = {o["name"].split(",")[0]: o for o in aw}
        for sf_item in sf:
            name = sf_item["name"]
            if name.endswith(":"):
                self.set_positive_matching(sf_item)
                continue
            if name in self.SF_TO_AW_MAP:
                name = self.SF_TO_AW_MAP[name]
            if name not in aw_by_names and name not in aw_by_short_names:
                words = (w.strip(" ,.%\n") for w in name.split(" "))
                # exclude for example "DC" from "Washington DC"
                words = filter(
                    lambda w: w and (len(w) != 2 or not w.isupper()), words)
                # exclude numbers from names, example: "
                # Peru 40%, Chile 60%" or "1. New York, NY, 2. Baltimore, MD"
                words = filter(lambda w: not w.isnumeric(), words)
                # combine back
                name = " ".join(words)
            if name in aw_by_names:
                self.set_positive_matching(sf_item)
                self.set_positive_matching(aw_by_names[name])
            elif name in aw_by_short_names:
                self.set_positive_matching(sf_item)
                self.set_positive_matching(aw_by_short_names[name])
        return aw, sf


class FlightSection(Section):
    def prepare_aw_field(self, setup_data):
        if setup_data:
            min_start, max_end, end_null = setup_data["min_start"],\
                                           setup_data["max_end"],\
                                           setup_data["end_null_count"]
            data = []
            if min_start:
                data.append(min_start)
            if max_end and not end_null:
                data.append(max_end)
            return data
        return []

    def prepare_sf_field(self, obj):
        data = []
        if obj.start:
            data.append(obj.start)
        if obj.end:
            data.append(obj.end)
        return data

    def run_comparison(self, aw, sf):
        if not all((aw, sf)):
            return aw, sf
        aw_start, sf_start = aw[0]["name"], sf[0]["name"]
        if aw_start >= sf_start:
            self.set_positive_matching(aw[0])
            self.set_positive_matching(sf[0])
        try:
            aw_end, sf_end = aw[1]["name"], sf[1]["name"]
        except IndexError:
            return aw, sf
        if aw_end == sf_end:
            self.set_positive_matching(aw[1])
            self.set_positive_matching(sf[1])
        return aw, sf


class TargetingSection(Section):
    RE_MARKETING = "Remarketing"
    INTEREST = "Interest"
    KEYWORD = "Keyword"
    CUSTOM_AFFINITY = "Custom Affinity"
    SF_TO_AW_MAP = {
        "3p Targeting": RE_MARKETING,
        "Custom CAS": RE_MARKETING,
        "Influencer Audience Extension": RE_MARKETING,
        "Client 1st Party Data": RE_MARKETING,
        "Consumer Patterns / Life Events": INTEREST,
        "Viral and Trending Keywords": KEYWORD,
    }

    def prepare_aw_field(self, setup_data):
        targeting_tactics = []
        if setup_data:
            for f in (
                    "targeting_interests", "targeting_topics",
                    "targeting_keywords", "targeting_channels",
                    "targeting_videos", "targeting_remarketings",
                    "targeting_custom_affinity"):
                if setup_data[f]:
                    parts = f.split("_")[1:]
                    value = " ".join([p.capitalize() for p in parts])
                    if value.endswith("s"):
                        value = value[:-1]
                    targeting_tactics.append(value)
        return list(sorted(targeting_tactics))

    def prepare_sf_field(self, obj):
        goal = obj.targeting_tactics.split(self.semicolon_sep)
        goal = set(f for f in goal if f)
        return list(sorted(goal))

    def run_comparison(self, aw, sf):
        for sf_item in sf:
            name = sf_item.get("name")
            if name in self.SF_TO_AW_MAP:
                name = self.SF_TO_AW_MAP.get(name)
            for aw_item in aw:
                if (aw_item.get("name") == name) or\
                        (name == self.CUSTOM_AFFINITY and
                         aw_item.get("name") == self.INTEREST):
                    self.set_positive_matching(aw_item)
                    self.set_positive_matching(sf_item)
                    continue
        return aw, sf


class TagsSection(Section):

    def run_comparison(self, aw, sf):
        if not (sf and aw):
            return
        sf_el_name, aw_el_name = sf[0].get("name"), aw[0].get("name")
        if sf_el_name == aw_el_name:
            self.set_positive_matching(sf[0])
            self.set_positive_matching(aw[0])
        return aw, sf

    def prepare_aw_field(self, setup_data):
        if setup_data and setup_data["tracking_template_is_set"]:
            if setup_data["tracking_template_is_set"] == setup_data["count"]:
                return ["Yes"]
            else:
                return ["{}/{}".format(setup_data["tracking_template_is_set"],
                                       setup_data["count"])]
        return ["No"]

    def prepare_sf_field(self, obj):
        if obj.tags == "Attached See Below":
            return ["Yes"]
        return ["No"]


class GDNSection(Section):
    fields = (
        "targeting_excluded_channels",
        "targeting_excluded_topics",
        "targeting_excluded_keywords")

    def run_comparison(self, aw, sf):
        if not (sf and aw):
            return
        sf_el_name, aw_el_name = sf[0].get("name"), aw[0].get("name")
        if sf_el_name == aw_el_name:
            self.set_positive_matching(sf[0])
            self.set_positive_matching(aw[0])
        return aw, sf

    def prepare_aw_field(self, setup_data):
        response = self.negative
        if setup_data:
            for f in self.fields:
                if setup_data[f] < setup_data["count"]:
                    break
            else:
                response = self.positive
        return [response]

    def prepare_sf_field(self, obj):
        if "GDN" in obj.types_of_targeting:
            return [self.positive]
        return [self.negative]
