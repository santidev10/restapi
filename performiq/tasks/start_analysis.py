from .constants import EXPORT_TYPES
from oauth.models import OAuthAccount
from performiq.analyzers.base_analyzer import PerformIQDataFetchError
from performiq.analyzers.constants import DataSourceType
from performiq.analyzers.executor_analyzer import ExecutorAnalyzer
from performiq.models import IQCampaign
from performiq.tasks.generate_exports import generate_exports
from performiq.utils.send_export_email import send_export_email
from saas import celery_app
from utils.datetime import now_in_default_tz


@celery_app.task
def start_analysis_task(iq_campaign_id: int, email: str, completion_link: str):
    iq_campaign = IQCampaign.objects.get(id=iq_campaign_id)
    iq_campaign.started = now_in_default_tz()

    try:
        executor_analyzer = ExecutorAnalyzer(iq_campaign)
    except PerformIQDataFetchError:
        # PerformIQDataFetchError will only be raised by Google Ads and DV360 analysis, which
        # always have a related campaign
        OAuthAccount.objects\
            .filter(oauth_type=iq_campaign.campaign.oauth_type, user=iq_campaign.user)\
            .update(is_enabled=False)
        all_results = {
            "error": "Unable to fetch data for analysis. Please re-OAuth."
        }
    else:
        if executor_analyzer.channel_analyses:
            executor_analyzer.analyze()
            all_results = executor_analyzer.get_results()

            export_results = generate_exports(iq_campaign)
            export_results.update(executor_analyzer.calculate_wastage_statistics())
            all_results["exports"] = export_results
            all_results["no_placement_analyzed"] = False
        else:
            all_results = {"no_placement_analyzed": True}
    iq_campaign.results = all_results
    iq_campaign.completed = now_in_default_tz()
    iq_campaign.save(update_fields=["started", "results", "completed"])

    if not iq_campaign.results.get("error"):
        _send_completion_email(email, iq_campaign, completion_link)


def _send_completion_email(email: str, iq_campaign: IQCampaign, completion_link: str):
    if not getattr(iq_campaign, "campaign"):
        export_type = EXPORT_TYPES[DataSourceType.CSV.value]
    else:
        oauth_type = iq_campaign.campaign.oauth_type
        export_type = EXPORT_TYPES[DataSourceType(oauth_type).value]
    send_export_email(export_type, [email], completion_link)
