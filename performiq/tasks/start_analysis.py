from performiq.analyzers.executor_analyzer import ExecutorAnalyzer
from performiq.models import IQCampaign
from performiq.tasks.generate_exports import generate_exports
from saas import celery_app
from utils.datetime import now_in_default_tz


@celery_app.task
def start_analysis_task(iq_campaign_id: int):
    iq_campaign = IQCampaign.objects.get(id=iq_campaign_id)
    iq_campaign.started = now_in_default_tz()

    executor_analyzer = ExecutorAnalyzer(iq_campaign)
    executor_analyzer.analyze()
    all_results = executor_analyzer.get_results()

    export_results = generate_exports(iq_campaign)
    export_results.update(executor_analyzer.calculate_wastage_statistics())
    all_results["exports"] = export_results

    iq_campaign.results = all_results
    iq_campaign.completed = now_in_default_tz()
    iq_campaign.save(update_fields=["results", "completed"])
