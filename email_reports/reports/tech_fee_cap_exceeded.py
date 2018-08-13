import logging
from decimal import Decimal

from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.db.models import Sum

from aw_reporting.models import OpPlacement, dict_add_calculated_stats
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from email_reports.reports.base import BaseEmailReport
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)

_body_template = "{ad_ops_name},\n{opportunity_name} is exceeding" \
                 " its tech fee cap of {tech_fee_cap} in the {placement_name}" \
                 " placement. Please adjust immediately."


class TechFeeCapExceeded(BaseEmailReport):

    def __init__(self, *args, **kwargs):
        super(TechFeeCapExceeded, self).__init__(*args, **kwargs)
        self.fake_tech_fee_cap = kwargs.get("fake_tech_fee_cap")

    def send(self):
        today = now_in_default_tz().date()

        placements = OpPlacement.objects.filter(
            opportunity__probability=100,
            start__lte=today, end__gte=today,
            dynamic_placement=DynamicPlacementType.RATE_AND_TECH_FEE,
            tech_fee_cap__isnull=False,
            tech_fee_type__isnull=False,
        ).values(
            "id", "name", "tech_fee_type", "tech_fee_cap",
            "opportunity_id", "opportunity__name",
            "opportunity__ad_ops_manager__email",
            "opportunity__ad_ops_manager__name",
            "opportunity__account_manager__email",
        ).order_by("id").annotate(
            video_views=Sum("adwords_campaigns__video_views"),
            impressions=Sum("adwords_campaigns__impressions"),
            cost=Sum("adwords_campaigns__cost"),
        )

        for placement in placements:

            dict_add_calculated_stats(placement)
            if placement["tech_fee_type"] == OpPlacement.TECH_FEE_CPV_TYPE:
                effective_rate = placement["average_cpv"]

            elif placement["tech_fee_type"] == OpPlacement.TECH_FEE_CPM_TYPE:
                effective_rate = placement["average_cpm"]
            else:
                logger.critical("Unknown tech fee type: {}".format(
                    placement["tech_fee_type"]))
                effective_rate = None

            if effective_rate is None:
                continue

            effective_rate = Decimal(format(effective_rate, ".4g"))

            tech_fee_cap = placement["tech_fee_cap"]
            if self.fake_tech_fee_cap is not None:
                tech_fee_cap = Decimal(self.fake_tech_fee_cap)

            if effective_rate > tech_fee_cap:

                to_recipients = []
                if placement["opportunity__ad_ops_manager__email"]:
                    to_recipients.append(
                        placement["opportunity__ad_ops_manager__email"])

                cc_recipients = list(settings.CF_AD_OPS_DIRECTORS)  # copy
                if placement["opportunity__account_manager__email"]:
                    cc_recipients.append(
                        placement["opportunity__account_manager__email"])

                manager_name = placement["opportunity__ad_ops_manager__name"]
                msg = EmailMultiAlternatives(
                    "Tech Fee Cap Exceeded",
                    _body_template.format(
                        ad_ops_name=manager_name or "Dear Manager",
                        opportunity_name=placement["opportunity__name"],
                        tech_fee_cap=tech_fee_cap,
                        placement_name=placement["name"],
                    ),
                    settings.EMAIL_HOST_USER,
                    to=self.get_to(to_recipients),
                    cc=self.get_cc(cc_recipients),
                    bcc=self.get_bcc(),
                    reply_to="",
                    from_email=settings.SENDER_EMAIL_ADDRESS,
                )
                msg.send(fail_silently=False)
