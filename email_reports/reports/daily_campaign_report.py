from datetime import timedelta

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.mail import EmailMultiAlternatives
from django.core.urlresolvers import reverse
from django.shortcuts import get_object_or_404
from django.template.defaultfilters import striptags
from django.template.loader import get_template

from aw_reporting.api.views.pacing_report.pacing_report_helper import \
    PacingReportHelper
from aw_reporting.models import User, SalesForceGoalType, Opportunity, \
    OpPlacement
from aw_reporting.reports.pacing_report import PacingReport
from email_reports.models import SavedEmail, get_uid
from email_reports.reports.base import BaseEmailReport
from utils.datetime import now_in_default_tz

spend_units_fields = ("today_goal", "yesterday_delivered",
                      "before_yesterday_delivered",
                      "today_goal_impressions", "today_goal_views",
                      "yesterday_delivered_views",
                      "yesterday_delivered_impressions",
                      "before_yesterday_delivered_views",
                      "before_yesterday_delivered_impressions")
spend_budget_fields = ("today_budget", "yesterday_budget",
                       "before_yesterday_budget")
spend_fields = spend_units_fields + spend_budget_fields


class DailyCampaignReport(BaseEmailReport):

    def __init__(self, *args, **kwargs):
        super(DailyCampaignReport, self).__init__(*args, **kwargs)

        self.today = now_in_default_tz().date()
        self.yesterday = self.today - timedelta(days=1)
        self.before_yesterday = self.yesterday - timedelta(days=1)

        self.roles = kwargs.get("roles", None)
        if self.roles:
            self.roles = self.roles.split(",")

    def is_on_going_placements(self, p):
        return None not in (p["goal_type_id"], p["start"], p["end"]) \
               and p["start"] <= self.today \
               and p["end"] >= self.before_yesterday \
               and p["goal_type_id"] < 2

    def send(self):

        html = get_template('daily_campaign_report.html')

        report = PacingReport()
        opportunities = report.get_opportunities({})

        for opportunity in opportunities:
            opportunity['type'] = "opportunity"
            PacingReportHelper.multiply_percents([opportunity])

            user_ids = list(filter(None, [(opportunity.get(k) or {}).get('id')
                                          for k in ("ad_ops", "am", "sales")]))
            users = User.objects.filter(id__in=user_ids)
            if self.roles:
                users = users.filter(role__name__in=self.roles)
            to_emails = users.values_list("email", flat=True)
            to_emails = set(filter(None, to_emails))
            exclude_emails = set(
                get_user_model().objects.filter(
                    email__in=to_emails,
                    is_subscribed_to_campaign_notifications=False
                ).values_list("email", flat=True)
            )
            to_emails = list(to_emails - exclude_emails)

            if not to_emails:
                continue

            context = self.get_context(opportunity, report)
            html_content = html.render(context)

            text_content = striptags(html_content)
            msg = EmailMultiAlternatives(
                context.get("title"),
                text_content,
                settings.EMAIL_HOST_USER,
                to=self.get_to(to_emails),
                bcc=self.get_bcc(),
            )
            msg.attach_alternative(html_content, "text/html")
            msg.send()

            # save the email
            SavedEmail.objects.create(id=context["email_uid"],
                                      html=html_content)

    def get_context(self, opportunity, report):
        title = "Daily Update for {}".format(opportunity['name'])

        email_uid = get_uid()
        web_view_url = reverse("email_reports_api_urls:email_report_web_view",
                               args=(email_uid,))
        opportunity_path = "/reports/opportunities/{opportunity_id}" \
            .format(opportunity_id=opportunity.get("id"))
        goal_type_ids = opportunity["goal_type_ids"]
        context = dict(
            title=title,
            email_uid=email_uid,
            email_link=self.host + web_view_url,
            opportunity_link=self.host + opportunity_path,
            unsubscribe_link=self.host + "/settings",
            today=self.today,
            yesterday=self.yesterday,
            before_yesterday=self.before_yesterday,
            opportunity=_map_opportunity(opportunity),
            is_cpm=SalesForceGoalType.CPM in goal_type_ids,
            is_cpv=SalesForceGoalType.CPV in goal_type_ids,
            host=self.host,
        )

        self._add_flight_data(opportunity, report)

        return context

    def _add_flight_data(self, opportunity, report):
        for f in spend_fields:
            opportunity[f] = 0

        opportunity_obj = get_object_or_404(Opportunity,
                                            id=opportunity['id'])
        placements = report.get_placements(opportunity_obj)
        placements = list(filter(self.is_on_going_placements, placements))

        for placement in placements:
            for f in spend_fields:
                placement[f] = 0 if f in spend_units_fields else 0.

            placement_obj = get_object_or_404(OpPlacement, id=placement['id'])
            flights = report.get_flights(placement_obj)

            for flight in flights:
                for f in spend_fields:
                    opportunity[f] += flight.get(f) or 0


def _map_opportunity(opportunity):
    opportunity["margin_str"] = _float_format(opportunity["margin"], 1, "%")
    opportunity["pacing_str"] = _float_format(opportunity["pacing"], 1, "%")
    opportunity["view_rate_str"] = _float_format(opportunity["video_view_rate"],
                                                 2, "%")
    opportunity["cpm_str"] = _float_format(opportunity["cpm"], 4)
    opportunity["cpv_str"] = _float_format(opportunity["cpv"], 4)
    return opportunity


def _float_format(value, float_round=1, suffix=""):
    if value is None:
        return "N/A"
    round_value = round(value, float_round)
    return "{}{}".format(round_value, suffix)
