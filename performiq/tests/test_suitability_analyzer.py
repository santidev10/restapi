from .utils import get_params
from performiq.analyzers import SuitabilityAnalyzer
from performiq.analyzers import ChannelAnalysis
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.int_iterator import int_iterator


class SuitabilityAnalyzerTestCase(ExtendedAPITestCase):
    def test_single_channel_score_passes(self):
        """ Test analysis that channel passes performance """
        _params = dict(
            score_threshold=50,
        )
        params = get_params(_params)
        data = params.copy()
        data["overall_score"] = 100
        analysis = ChannelAnalysis(f"channel_id_{next(int_iterator)}", data=data)
        analyzer = SuitabilityAnalyzer(params)
        result = analyzer.analyze(analysis)
        self.assertEqual(result["passed"], True)
        self.assertEqual(analysis.clean, True)

    def test_single_channel_score_fails(self):
        """ Test analysis that channel fails suitability """
        _params = dict(
            score_threshold=50,
        )
        params = get_params(_params)
        analysis = ChannelAnalysis(f"channel_id_{next(int_iterator)}", data={"overall_score": 25})
        analyzer = SuitabilityAnalyzer(params)
        result = analyzer.analyze(analysis)
        self.assertEqual(result["passed"], False)
        self.assertEqual(analysis.clean, False)
    
    def test_result_counts(self):
        _params = dict(
            score_threshold=50,
        )
        params = get_params(_params)
        analyzer = SuitabilityAnalyzer(params)
        data = [
            # fails
            dict(overall_score=30),
            dict(overall_score=40),

            # passes
            dict(overall_score=50),
            dict(overall_score=60),
            dict(overall_score=70),
        ]
        analyses = [
            ChannelAnalysis(f"channel_id_{next(int_iterator)}", data=d)
            for d in data
        ]
        for analysis in analyses:
            analyzer.analyze(analysis)
        results = analyzer.get_results()
        self.assertEqual(results["passed"], 3)
        self.assertEqual(results["failed"], 2)
        self.assertEqual(results["overall_score"], 3 / 5 * 100)
