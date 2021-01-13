from .utils import get_params
from performiq.analyzers import PerformanceAnalyzer
from performiq.analyzers import ChannelAnalysis
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.int_iterator import int_iterator


class PerformanceAnalyzerTestCase(ExtendedAPITestCase):
    def _setup(self):
        pass

    def test_single_channel_score_passes(self):
        """ Test analysis that channel passes performance. Every metric compared to params must be >= """
        _params = dict(
            active_view_viewability=50,
            average_cpm=50,
            average_cpv=0.75,
            ctr=0.75,
            video_view_rate=50,
            video_quartile_100_rate=50
        )
        params = get_params(_params)
        analysis = ChannelAnalysis(f"channel_id_{next(int_iterator)}", data=_params)
        analyzer = PerformanceAnalyzer(params)
        result = analyzer.analyze(analysis)
        self.assertEqual(result["passed"], True)
        self.assertEqual(analysis.clean, True)

    def test_single_channel_score_fails(self):
        """ Test analysis that channel fails performance. If one metric is < params, then entire analysis fails """
        _params = dict(
            active_view_viewability=50,
            average_cpm=50,
            average_cpv=0.75,
            ctr=0.75,
            video_view_rate=50,
            video_quartile_100_rate=50
        )
        params = get_params(_params)

        data = _params.copy()
        data["average_cpm"] = 0
        analysis = ChannelAnalysis(f"channel_id_{next(int_iterator)}", data=data)
        analyzer = PerformanceAnalyzer(params)
        result = analyzer.analyze(analysis)
        self.assertEqual(result["passed"], False)
        self.assertEqual(analysis.clean, False)

    def test_averages(self):
        """ Test calculating overall averages of channels analyzed """
        params = dict(
            average_cpm=5,
            average_cpv=0.75,
        )
        analyzer = PerformanceAnalyzer(params)
        data = [
            dict(average_cpm=2, average_cpv=5),
            dict(average_cpm=4, average_cpv=10),
            dict(average_cpm=6, average_cpv=15),
        ]
        analyses = [
            ChannelAnalysis(f"channel_id_{next(int_iterator)}", data=d)
            for d in data
        ]
        for analysis in analyses:
            analyzer.analyze(analysis)
        results = analyzer.get_results()
        avg_cpm = sum(a.get("average_cpm") for a in analyses) / len(analyses)
        avg_cpv = sum(a.get("average_cpv") for a in analyses) / len(analyses)
        self.assertAlmostEqual(results["average_cpm"]["avg"], avg_cpm)
        self.assertAlmostEqual(results["average_cpv"]["avg"], avg_cpv)

    def test_overall_performance(self):
        _params = dict(
            active_view_viewability=1,
            average_cpm=1,
            average_cpv=1,
            ctr=1,
            video_view_rate=1,
            video_quartile_100_rate=1,
        )
        params = get_params(_params)
        data = [
            # should fail
            dict(
                active_view_viewability=0,
                average_cpm=0,
                average_cpv=0,
                ctr=0,
                video_view_rate=0,
                video_quartile_100_rate=0,
            ),
            # should fail
            dict(
                active_view_viewability=0,
                average_cpm=0,
                average_cpv=0,
                ctr=0,
                video_view_rate=0,
                video_quartile_100_rate=0,
            ),
            # should pass
            dict(
                active_view_viewability=5,
                average_cpm=5,
                average_cpv=5,
                ctr=5,
                video_view_rate=5,
                video_quartile_100_rate=5,
            ),
            # should pass
            dict(
                active_view_viewability=5,
                average_cpm=5,
                average_cpv=5,
                ctr=5,
                video_view_rate=5,
                video_quartile_100_rate=5,
            ),
            # should pass
            dict(
                active_view_viewability=5,
                average_cpm=5,
                average_cpv=5,
                ctr=5,
                video_view_rate=5,
                video_quartile_100_rate=5,
            ),
        ]
        analyzer = PerformanceAnalyzer(params)
        analyses = [
            ChannelAnalysis(f"channel_id_{next(int_iterator)}", data=d)
            for d in data
        ]
        for analysis in analyses:
            analyzer.analyze(analysis)
        results = analyzer.get_results()
        for metric_result in results.values():
            if isinstance(metric_result, dict):
                self.assertEqual(metric_result["failed"], 2)
                self.assertEqual(metric_result["passed"], 3)
                self.assertAlmostEqual(metric_result["performance"], 60)
        self.assertAlmostEqual(results["overall_score"], 60)

    def test_not_analyzed_no_threshold(self):
        """ Test metric is not analyzed if a threshold is not set """
        params = dict(
            average_cpm=1,
            average_cpv=None,
        )
        analyzer = PerformanceAnalyzer(params)
        analysis = ChannelAnalysis(f"channel_id_{next(int_iterator)}", data=dict(average_cpm=1, average_cpv=1))
        analyzer.analyze(analysis)
        results = analyzer.get_results()
        self.assertIsNone(results["average_cpv"]["performance"])

    def test_no_metric_ignore(self):
        """ Test that if analysis does not have metric, it does not contribute to overall performance """
        params = dict(
            ctr=0.75,
            video_view_rate=50,
        )
        analyzer = PerformanceAnalyzer(params)
        data = [
            dict(ctr=0.75, video_view_rate=50),
            dict(ctr=0.75, video_view_rate=0),
            # Not having video_view_rate should not factor into performance
            dict(ctr=0),
        ]
        analyses = [
            ChannelAnalysis(f"channel_id_{next(int_iterator)}", data=d) for d in data
        ]
        for a in analyses:
            analyzer.analyze(a)
        results = analyzer.get_results()
        # All items have ctr, two pass, one fail, so should be average of 3
        self.assertAlmostEqual(results["ctr"]["performance"], round(2 / 3 * 100, 2), delta=1)
        # Only two items in data have video_view_rate, one pass one fail, so should be average of 2
        self.assertAlmostEqual(results["video_view_rate"]["performance"], round(1 / 2 * 100, 2), delta=1)

    def test_comparison_direction(self):
        """ Test that average_cpm and average_cpv fields pass when lower than threshold as lower cost is favorable """
        params = dict(
            average_cpm=1.5,
            average_cpv=0.02,
        )
        analyzer = PerformanceAnalyzer(params)
        data = [
            # Fails average_cpm, passes average_cpv
            dict(average_cpm=2.00, average_cpv=0.01),
            # Passes average_cpm, fails average_cpv
            dict(average_cpm=1.3, average_cpv=0.5),
        ]
        analyses = [
            ChannelAnalysis(f"channel_id_{next(int_iterator)}", data=d) for d in data
        ]
        for a in analyses:
            analyzer.analyze(a)
        results = analyzer.get_results()
        self.assertTrue(results["average_cpm"]["passed"] == results["average_cpv"]["passed"] == 1)
        self.assertTrue(results["average_cpm"]["failed"] == results["average_cpv"]["failed"] == 1)
        self.assertAlmostEqual((data[0]["average_cpm"] + data[1]["average_cpm"]) / 2, results["average_cpm"]["avg"])
        self.assertAlmostEqual((data[0]["average_cpv"] + data[1]["average_cpv"]) / 2, results["average_cpv"]["avg"])
        # Failing in any one benchmark fails entire analysis
        self.assertEqual(results["overall_score"], 0)
