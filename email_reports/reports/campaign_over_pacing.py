from email_reports.reports.base_campaign_pacing_report import \
    BaseCampaignPacingEmailReport


class CampaignOverPacing(BaseCampaignPacingEmailReport):
    _problem_str = 'over'

    def _is_risky_pacing(self, pacing):
        return pacing > (1. + self.pacing_bound)
