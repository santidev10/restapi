from .utils import get_params
from .utils import get_test_analyses
from performiq.analyzers import ContextualAnalyzer
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.int_iterator import int_iterator
from es_components.models import Channel


class ContexualAnalyzerTestCase(ExtendedAPITestCase):
    def _setup(self):
        pass

    def _get_analyses(self, data: list):
        channels = []
        for d in data:
            channel = Channel(f"channel_id_{next(int_iterator)}".zfill(24))
            channel.populate_general_data(iab_categories=d.get("content_categories"), top_lang_code=d.get("lang_code"))
            channel.populate_task_us_data(content_type=d.get("content_type"), content_quality=d.get("content_quality"))
            channels.append(channel)
        analyses = get_test_analyses(channels)
        return analyses

    def test_single_channel_score_passes(self):
        """ Test analysis that channel passes performance. Every value compared to params checks for membership """
        _params = dict(
            content_categories=["Movies"],
            content_quality=[0],
            content_type=[0],
            languages=["en"],
        )
        params = get_params(_params)
        data = dict(
            content_categories=["Movies"],
            content_quality="0",
            content_type="0",
            lang_code="en",
        )
        analysis = self._get_analyses([data])[0]
        analyzer = ContextualAnalyzer(params)
        result = analyzer.analyze(analysis)
        self.assertEqual(result["passed"], True)
        self.assertEqual(analysis.clean, True)

    def test_single_channel_score_fails(self):
        """ Test analysis that channel fails contexual. If one value does not match params, entire channel fails """
        _params = dict(
            content_categories=["Movies"],
            content_quality=[0],
            content_type=[0],
            languages=["en"],
        )
        params = get_params(_params)
        data = dict(
            content_categories=["Automotive"],
            content_quality="0",
            content_type="0",
            lang_code="en",
        )
        analysis = self._get_analyses([data])[0]
        analyzer = ContextualAnalyzer(params)
        result = analyzer.analyze(analysis)
        self.assertEqual(result["passed"], False)
        self.assertEqual(analysis.clean, False)

    def test_not_analyzed_no_threshold(self):
        """ Test metric is not analyzed if params for it is not set """
        _params = dict(
            content_categories=[],
            content_quality=[],
            content_type=[0],
            languages=["en"],
        )
        params = get_params(_params)
        analyzer = ContextualAnalyzer(params)
        data = dict(
            content_categories=["Music"],
            content_quality="1",
            content_type="0",
            lang_code="en",
        )
        analysis = self._get_analyses([data])[0]
        analyzer.analyze(analysis)
        results = analyzer.get_results()

        # Matched should be 0 as content_categories was not set on params
        self.assertEqual(results["content_categories"]["total_matched_percent"], 0)
        # Occurrences should still be counted if set in params
        categories = set(r["category"] for r in results["content_categories"]["category_occurrence"])
        self.assertEqual(results["overall_score"], 100)
        # These assertions represent percentage occurrence
        self.assertEqual(results["content_quality"][0].get(1), 100)
        self.assertEqual(results["content_type"][0].get(0), 100)
        self.assertEqual(results["languages"][0]["en"], 100)

    def test_matched_categories(self):
        """
        Test matched percentage calculation
        If a channel contains many categories, then just one match of any category counts as a match
        """
        _params = dict(
            content_categories=["Music", "Movies", "Television"]
        )
        params = get_params(_params)
        analyzer = ContextualAnalyzer(params)
        # Total matched should be two, as two channels contain at least one content category
        data = [
            dict(content_categories=["Music", "Movies", "Television"]),
            dict(content_categories=["Music", "Instruments"]),
            dict(content_categories=["Cars", "Automobiles", "Driving"]),
        ]
        analyses = self._get_analyses(data)
        for analysis in analyses:
            analyzer.analyze(analysis)
        results = analyzer.get_results()
        content_category_results = results["content_categories"]["category_occurrence"]
        self.assertEqual(results["content_categories"]["total_matched_percent"], round(2 / 3 * 100, 4))
        self.assertEqual(content_category_results[0]["category"], "Music")
        # Music matched twice out of three items
        self.assertAlmostEqual(results["content_categories"]["category_occurrence"][0]["percent_occurrence"],
                               2 / 3 * 100, places=4)

        # Remaining categories only occur once
        for category_data in content_category_results[1:]:
            with self.subTest(f"Test occurrence: {category_data['category']}"):
                self.assertAlmostEqual(category_data["percent_occurrence"], 1 / 3 * 100, places=4)

        all_categories = set()
        for d in data:
            all_categories.update(d["content_categories"])
        seen_categories = set(r["category"] for r in results["content_categories"]["category_occurrence"])
        self.assertEqual(all_categories, seen_categories)

    def test_percentages(self):
        """ Test percentage occurrences are sorted and calculated correctly """
        _params = dict(
            languages=["en", "ko"],
            content_quality=[0],
            content_type=[0],
        )
        params = get_params(_params)
        analyzer = ContextualAnalyzer(params)
        data = [
            dict(lang_code="ko", content_type="0", content_quality="2"),
            dict(lang_code="en", content_type="0", content_quality="2"),
            dict(lang_code="en", content_type="0", content_quality="2"),
            dict(lang_code="en", content_type="1", content_quality="0"),
        ]
        analyses = self._get_analyses(data)
        for analysis in analyses:
            analyzer.analyze(analysis)
        results = analyzer.get_results()
        # en should be sorted first with 75% occurrence
        self.assertEqual(results["languages"][0]["en"], 75)
        self.assertEqual(results["languages"][1]["ko"], 25)
        # Content quality of type 2 is sorted first, with 75% occurrence
        self.assertEqual(results["content_quality"][0].get(2), 75)
        self.assertEqual(results["content_quality"][1].get(0), 25)

        # Content type of type 0 is sorted first, with 75% occurrence
        self.assertEqual(results["content_type"][0].get(0), 75)
        self.assertEqual(results["content_type"][1].get(1), 25)

    def test_none_values_passed(self):
        """ Test that analyzing None values should not fail placement """
        _params = dict(
            lang_code=["en", "ko"],
            content_quality=[0],
            content_type=[0],
        )
        params = get_params(_params)
        analyzer = ContextualAnalyzer(params)
        data = [
            dict(lang_code="ko", content_type=None),
            dict(lang_code="en", content_quality=None),
            dict(lang_code="en", content_type="0", content_quality="0"),
        ]
        analyses = self._get_analyses(data)
        for a in analyses:
            analyzer.analyze(a)
        results = analyzer.get_results()
        self.assertEqual(results["overall_score"], 100)

    def test_contextual_unknown(self):
        """
        Test that if unknown values are analyzed correctly if targeted
        IQCampaign param values of -1 indicate unknown (None) values are targeted
        """
        _params = dict(
            content_quality=[-1],
            content_type=[-1],
        )
        params = get_params(_params)
        analyzer = ContextualAnalyzer(params)
        data = [
            dict(content_type=None, content_quality=None)
            for _ in range(2)
        ]
        analyses = self._get_analyses(data)
        for a in analyses:
            analyzer.analyze(a)
        results = analyzer.get_results()
        self.assertEqual(results["overall_score"], 100)