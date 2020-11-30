from .utils import get_params
from performiq.analyzers import ContextualAnalyzer
from performiq.analyzers import ChannelAnalysis
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.int_iterator import int_iterator


class ContexualAnalyzerTestCase(ExtendedAPITestCase):
    def _setup(self):
        pass

    def test_single_channel_score_passes(self):
        """ Test analysis that channel passes performance. Every value compared to params checks for membership """
        _params = dict(
            content_categories=["Movies"],
            content_quality=["0"],
            content_type=["0"],
            languages=["en"],
        )
        params = get_params(_params)
        analysis = ChannelAnalysis(f"channel_id_{next(int_iterator)}", data=_params)
        analyzer = ContextualAnalyzer(params)
        result = analyzer.analyze(analysis)
        self.assertEqual(result["passed"], True)
        self.assertEqual(analysis.clean, True)

    def test_single_channel_score_fails(self):
        """ Test analysis that channel fails contexual. If one value does not match params, entire channel fails """
        _params = dict(
            content_categories=["Movies"],
            content_quality=["0"],
            content_type=["0"],
            languages=["en"],
        )
        params = get_params(_params)
        data = params.copy()
        data["content_categories"] = ["Automotive"]
        analysis = ChannelAnalysis(f"channel_id_{next(int_iterator)}", data=data)
        analyzer = ContextualAnalyzer(params)
        result = analyzer.analyze(analysis)
        self.assertEqual(result["passed"], False)
        self.assertEqual(analysis.clean, False)

    def test_not_analyzed_no_threshold(self):
        """ Test metric is not analyzed if params for it is not set """
        _params = dict(
            content_categories=[],
            content_quality=[],
            content_type=["0"],
            languages=["en"],
        )
        params = get_params(_params)
        analyzer = ContextualAnalyzer(params)

        data = _params.copy()
        data["content_categories"] = ["Music"]
        data["content_quality"] = ["1"]
        analysis = ChannelAnalysis(f"channel_id_{next(int_iterator)}", data=data)
        analyzer.analyze(analysis)
        results = analyzer.get_results()

        # Matched should be 0 as content_categories was not set on params
        self.assertEqual(results["content_categories"]["total_matched_percent"], 0)
        # Occurrences should still be counted if set in params
        categories = set(r["category"] for r in results["content_categories"]["category_occurrence"])
        self.assertTrue("Music" not in categories)
        self.assertEqual(results["overall_score"], 100)
        # These assertions represent percentage occurrence
        self.assertEqual(results["content_quality"][0].get("1"), 100)
        self.assertEqual(results["content_type"][0].get("0"), 100)
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
            dict(content_categories=["Music"]),
            dict(content_categories=["Cars"]),
        ]
        analyses = [
            ChannelAnalysis(f"channel_id_{next(int_iterator)}", data=d)
            for d in data
        ]
        for analysis in analyses:
            analyzer.analyze(analysis)
        results = analyzer.get_results()
        self.assertEqual(results["content_categories"]["total_matched_percent"], round(2 / 3 * 100, 4))
        self.assertEqual(results["content_categories"]["category_occurrence"][0]["category"], "Music")
        categories = set(r["category"] for r in results["content_categories"]["category_occurrence"])
        self.assertEqual(set(params["content_categories"]), categories)

    def test_percentages(self):
        """ Test percentage occurrences are sorted and calculated correctly """
        _params = dict(
            languages=["en", "ko"],
            content_quality=["0"],
            content_type=["0"],
        )
        params = get_params(_params)
        analyzer = ContextualAnalyzer(params)
        data = [
            dict(languages="ko", content_type="0", content_quality="2"),
            dict(languages="en", content_type="0", content_quality="2"),
            dict(languages="en", content_type="0", content_quality="2"),
            dict(languages="en", content_type="1", content_quality="0"),
        ]
        analyses = [
            ChannelAnalysis(f"channel_id_{next(int_iterator)}", data=d)
            for d in data
        ]
        for analysis in analyses:
            analyzer.analyze(analysis)
        results = analyzer.get_results()
        # en should be sorted first with 75% occurrence
        self.assertEqual(results["languages"][0]["en"], 75)
        self.assertEqual(results["languages"][1]["ko"], 25)
        # Content quality of type 2 is sorted first, with 75% occurrence
        self.assertEqual(results["content_quality"][0].get("2"), 75)
        self.assertEqual(results["content_quality"][1].get("0"), 25)

        # Content type of type 0 is sorted first, with 75% occurrence
        self.assertEqual(results["content_type"][0].get("0"), 75)
        self.assertEqual(results["content_type"][1].get("1"), 25)
