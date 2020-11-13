from performiq.analyzers.executor_analyzer import ExecutorAnalyzer
from performiq.models import IQCampaign
from performiq.tasks.generate_exports import generate_exports
from saas import celery_app


@celery_app.task
def analyze(iq_campaign_id: str):
    iq_campaign = IQCampaign.objects.get(id=iq_campaign_id)

    executor_analyzer = ExecutorAnalyzer(iq_campaign)
    executor_analyzer.analyze()
    all_results = executor_analyzer.get_results()

    export_results = generate_exports(iq_campaign)
    export_results.update(executor_analyzer.calculate_wastage_statistics())
    all_results["exports"] = export_results
    iq_campaign.results = all_results
    iq_campaign.save(update_fields=["results"])
