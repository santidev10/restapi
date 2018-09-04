import logging
from datetime import datetime
from datetime import timedelta
from io import BytesIO
from typing import Dict
from typing import List
from typing import Set

import xlsxwriter
from django.conf import settings
from django.core.mail import EmailMessage
from django.core.management import BaseCommand
from django.db import transaction
from django.http import QueryDict

import boto3

from audit_tool.adwords import AdWords
from audit_tool.dmo import AccountDMO
from audit_tool.dmo import VideoDMO
from audit_tool.keywords import Keywords
from audit_tool.models import KeywordAudit
from audit_tool.models import VideoAudit

from audit_tool.youtube import Youtube
from aw_reporting.models import AWConnection
from aw_reporting.models import Account
from singledb.connector import SingleDatabaseApiConnector
from utils.datetime import now_in_default_tz

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    # CL arguments --->
    date_start = None
    date_finish = None
    account_ids = None
    # <--- CL arguments

    accounts = None
    accounts_dict = None

    s3_folder = "daily-audit-reports"

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--date_start",
            dest="date_start",
            help="Date start (YYYYMMDD)",
            type=str,
            default=None,
        )
        parser.add_argument(
            "--date_finish",
            dest="date_finish",
            help="Date finish (YYYYMMDD)",
            type=str,
            default=None,
        )
        parser.add_argument(
            "--account_ids",
            dest="account_ids",
            help="Account IDs as a comma separated string",
            type=str,
            default=None,
        )

    def load_arguments(self, *args, **options) -> None:
        yesterday = now_in_default_tz() - timedelta(days=1)
        yesterday = yesterday.date()

        # argument: date_start
        self.date_start = options.get("date_start")
        if not self.date_start:
            self.date_start = yesterday.strftime("%Y%m%d")

        # argument: date_finish
        self.date_finish = options.get("date_finish")
        if not self.date_finish:
            self.date_finish = yesterday.strftime("%Y%m%d")

        # argument: account_ids
        account_ids = options.get("account_ids")
        if account_ids:
            self.account_ids = account_ids.split(",")

    def handle(self, *args, **options) -> None:
        self.load_arguments(*args, **options)

        logger.info("Starting daily audit")
        VideoAudit.objects.cleanup()
        KeywordAudit.objects.cleanup()

        self.accounts = self.load_accounts()
        self.accounts_dict = {_.account_id: _.name for _ in self.accounts}

        for date in self.get_dates():
            # get data from AdWords API
            adwords = AdWords(accounts=self.accounts, date=date, download=True)
            reports = adwords.get_video_reports()
            video_ids = reports.keys()

            # get data from Data API
            youtube = Youtube()
            youtube.download(video_ids)
            videos = [i for i in youtube.get_all_items()]

            # parse by keywords
            self.parse_videos_by_keywords(videos)

            # get preferred channels list
            preferred_channels = self.load_preferred_channels()

            # save results and send report
            self.save_and_send(date, videos, reports, preferred_channels)

        logger.info("Done")

    def get_dates(self) -> str:
        start = datetime.strptime(self.date_start, "%Y%m%d")
        finish = datetime.strptime(self.date_finish, "%Y%m%d")
        date = start
        day = timedelta(days=1)
        while date <= finish:
            yield date.strftime("%Y%m%d")
            date += day

    def load_accounts(self) -> List[AccountDMO]:
        logger.info("Loading accounts")
        accounts = []

        # load names and managers
        names = {}
        managers = {}
        queryset = Account.objects.filter(can_manage_clients=False)\
                                  .values("id", "name", "managers")\
                                  .order_by("id")

        if self.account_ids is not None:
            queryset = queryset.filter(id__in=self.account_ids)

        for account in queryset:
            account_id = account["id"]
            name = account["name"]
            names[account_id] = name
            if account_id not in managers:
                managers[account_id] = []
            managers[account_id].append(account["managers"])

        # load connections
        refresh_tokens = {}
        queryset = AWConnection.objects.filter(
            mcc_permissions__can_read=True,
            revoked_access=False,
        ).values("mcc_permissions__account", "refresh_token")

        for connection in queryset:
            account_id = connection["mcc_permissions__account"]
            refresh_tokens[account_id] = connection["refresh_token"]

        # prepare accounts
        for account_id in sorted(managers.keys()):
            tokens = [refresh_tokens[_] for _ in managers[account_id]
                      if _ in refresh_tokens]
            accounts.append(
                AccountDMO(
                    account_id=account_id,
                    name=names[account_id],
                    refresh_tokens=tokens,
                )
            )
        logger.info("Loaded {} account(s)".format(len(accounts)))
        return accounts

    @staticmethod
    def load_preferred_channels() -> Set[str]:
        logger.info("Loading preferred channels list")
        singledb = SingleDatabaseApiConnector()
        response = singledb.execute_get_call(
            "channels/",
            QueryDict("fields=channel_id&preferred__term=1&size=10000")
        )
        preferred_channels = set([_['channel_id'] for _ in response['items']])
        count = len(preferred_channels)
        logger.info("Loaded {} preferred channel(s)".format(count))
        return preferred_channels

    @staticmethod
    def parse_videos_by_keywords(videos: List[VideoDMO]) -> None:
        logger.info("Parsing {} video(s)".format(len(videos)))
        keywords = Keywords()
        keywords.load_from_sdb()
        keywords.compile_regexp()
        texts = [video.get_text() for video in videos]
        found = keywords.parse_all(texts)
        for idx, video in enumerate(videos):
            video.found = found[idx]
        logger.info("Parsed {} video(s)".format(len(videos)))

    def save_and_send(self, date: str,
                            videos: List[VideoDMO],
                            reports: Dict[str, list],
                            preferred_channels: Set[str]) -> None:

        logger.info("Storing XLSX")

        # create workbook
        output = BytesIO()
        workbook = xlsxwriter.Workbook(output, {
            "in_memory": True,
            "strings_to_urls": False,
        })
        header_format = workbook.add_format({
            "bold": True,
            "align": "center",
            "bg_color": "#C0C0C0",
            "border": True,
        })
        numberic_format = workbook.add_format({
            "align": "right",
            "num_format": "0",
        })
        percentage_format = workbook.add_format({
            "align": "right",
            "num_format": "0.00%",
        })
        text_format = workbook.add_format({
            "text_wrap": True
        })

        audit_date = datetime.strptime(date, "%Y%m%d")

        # add sheet: Keywords Hits
        videos_to_save = []
        worksheet = workbook.add_worksheet("Keywords Hits (all)")
        fields = (
            ("VideoTitle", 90),
            ("VideoUrl", 40),
            ("ChannelTitle", 45),
            ("ChannelUrl", 55),
            ("Google Preferred", 14),
            ("Impressions", 10),
            ("Sentiment Analysis", 16),
            ("Hits", 10),
            ("Words that hit", 20),
            ("Account Info", 200),
        )
        for x, field in enumerate(fields):
            worksheet.write(0, x, field[0], header_format)
            worksheet.set_column(x, x, field[1])

        sorted_videos = sorted(videos, key=lambda _: -len(_.found))
        for y, item in enumerate(sorted_videos):
            hits = len(item.found)
            if hits < 5:
                break
            data = reports[item.id]
            impressions = sum([int(r.get("Impressions")) for r in data])
            words = ",".join(item.found)
            worksheet.write(y+1, 0, item.title)
            worksheet.write(y+1, 1, "'" + item.url)
            worksheet.write(y+1, 2, item.channel_title)
            worksheet.write(y+1, 3, "'" + item.channel_url)
            if item.channel_id in preferred_channels:
                worksheet.write(y+1, 4, "YES")
            worksheet.write(y+1, 5, impressions, numberic_format)
            worksheet.write(y+1, 6, item.sentiment, percentage_format)
            worksheet.write(y+1, 7, hits, numberic_format)
            worksheet.write(y+1, 8, words)

            account_info = self._get_account_info(data)
            text_account_info = "\n".join(account_info)
            worksheet.write(y+1, 9, text_account_info, text_format)
            if len(account_info) > 1:
                worksheet.set_row(y+1, 15*len(account_info))
            video_audit = VideoAudit(
                date=audit_date,
                video_id=item.id,
                video_title=item.title,
                channel_id=item.channel_id,
                channel_title=item.channel_title or "No title",
                preferred=item.channel_id in preferred_channels,
                impressions=impressions,
                sentiment=item.sentiment,
                hits=hits,
                account_info=text_account_info,
                words=words,
            )
            videos_to_save.append(video_audit)
        with transaction.atomic():
            VideoAudit.objects.filter(date=audit_date).delete()
            VideoAudit.objects.bulk_create(videos_to_save)

        # add sheet: Keywords Hits
        old_videos = VideoAudit.objects.filter(date__lt=audit_date).values_list("video_id", flat=True)
        old_videos = set(old_videos)

        worksheet = workbook.add_worksheet("Keywords Hits")
        for x, field in enumerate(fields):
            worksheet.write(0, x, field[0], header_format)
            worksheet.set_column(x, x, field[1])

        y = 0
        new_videos = set()
        for item in sorted_videos:
            hits = len(item.found)
            if hits < 5:
                break
            if item.id in old_videos:
                continue
            new_videos.add(item.id)
            data = reports[item.id]
            impressions = sum([int(r.get("Impressions")) for r in data])
            words = ",".join(item.found)
            worksheet.write(y+1, 0, item.title)
            worksheet.write(y+1, 1, "'" + item.url)
            worksheet.write(y+1, 2, item.channel_title)
            worksheet.write(y+1, 3, "'" + item.channel_url)
            if item.channel_id in preferred_channels:
                worksheet.write(y+1, 4, "YES")
            worksheet.write(y+1, 5, impressions, numberic_format)
            worksheet.write(y+1, 6, item.sentiment, percentage_format)
            worksheet.write(y+1, 7, hits, numberic_format)
            worksheet.write(y+1, 8, words)

            account_info = self._get_account_info(data)
            text_account_info = "\n".join(account_info)
            worksheet.write(y+1, 9, text_account_info, text_format)
            if len(account_info) > 1:
                worksheet.set_row(y+1, 15*len(account_info))
            y += 1

        # add sheet: Keywords
        keywords_to_save = []
        worksheet = workbook.add_worksheet("Keywords")
        fields = (
            ("Keyword", 45),
            ("Videos", 10),
            ("Impressions", 10),
        )
        for x, field in enumerate(fields):
            worksheet.write(0, x, field[0], header_format)
            worksheet.set_column(x, x, field[1])

        keyword_videos = {}
        for item in videos:
            for keyword in item.found:
                if keyword not in keyword_videos:
                    keyword_videos[keyword] = set()
                keyword_videos[keyword].add(item.id)
        result = []
        for keyword, videos_info in keyword_videos.items():
            impressions = 0
            for video in videos_info:
                impressions += sum(
                    [int(r.get("Impressions")) for r in reports[video]]
                )
            result.append((keyword, len(videos_info), impressions))
        result = sorted(result, key=lambda _: -_[1])
        for y, _ in enumerate(result):
            keyword, videos_count, impressions = _
            worksheet.write(y+1, 0, keyword)
            worksheet.write(y+1, 1, videos_count, numberic_format)
            worksheet.write(y+1, 2, impressions, numberic_format)
            keyword_audit = KeywordAudit(
                date=audit_date,
                keyword=keyword,
                videos=videos_count,
                impressions=impressions,
            )
            keywords_to_save.append(keyword_audit)
        with transaction.atomic():
            KeywordAudit.objects.filter(date=audit_date).delete()
            KeywordAudit.objects.bulk_create(keywords_to_save)

        # add sheet: Google Preferred
        worksheet = workbook.add_worksheet("Google Preferred")
        fields = (
            ("VideoTitle", 90),
            ("VideoUrl", 40),
            ("ChannelTitle", 45),
            ("ChannelUrl", 55),
            ("Google Preferred", 14),
            ("Impressions", 10),
            ("Sentiment Analysis", 16),
            ("Hits", 10),
            ("Words that hit", 20),
            ("Account Info", 200),
        )
        for x, field in enumerate(fields):
            worksheet.write(0, x, field[0], header_format)
            worksheet.set_column(x, x, field[1])

        sorted_videos = sorted(videos, key=lambda _: -len(_.found))
        for y, item in enumerate(sorted_videos):
            hits = len(item.found)
            if hits < 5 and item.channel_id in preferred_channels:
                break
            data = reports[item.id]
            impressions = sum([int(r.get("Impressions")) for r in data])
            words = ",".join(item.found)
            worksheet.write(y+1, 0, item.title)
            worksheet.write(y+1, 1, "'" + item.url)
            worksheet.write(y+1, 2, item.channel_title)
            worksheet.write(y+1, 3, "'" + item.channel_url)
            worksheet.write(y+1, 4, "YES")
            worksheet.write(y+1, 5, impressions, numberic_format)
            worksheet.write(y+1, 6, item.sentiment, percentage_format)
            worksheet.write(y+1, 7, hits, numberic_format)
            worksheet.write(y+1, 8, words)

            account_info = self._get_account_info(data)
            text_account_info = "\n".join(account_info)
            worksheet.write(y+1, 9, text_account_info, text_format)
            if len(account_info) > 1:
                worksheet.set_row(y+1, 15*len(account_info))

        # close workbook
        workbook.worksheets_objs[0], workbook.worksheets_objs[1] =\
            workbook.worksheets_objs[1], workbook.worksheets_objs[0]
        workbook.worksheets_objs[1].hide()
        workbook.close()
        xlsx_data = output.getvalue()
        logger.info("XLSX is ready")

        # calculate totals
        logger.info("Calculating totals")
        totals = {"impressions": 0}
        for report in reports.values():
            impressions = sum([int(_.get("Impressions")) for _ in report])
            totals["impressions"] += impressions
        totals["videos"] = len(set(_.id for _ in videos))
        totals["new_videos"] = len(new_videos)
        totals["channels"] = len(set(_.channel_id for _ in videos))

        filename = "daily_audit_{}.xlsx".format(date)

        # Save to S3
        self.save_se(filename, xlsx_data)

        # prepare E-mail
        subject = "Daily Audit {}".format(date)
        body = "Total impressions: {impressions}\n" \
               "Total videos (new): {videos} ({new_videos})\n" \
               "Total channels: {channels}\n".format(**totals)

        # E-mail
        logger.info("Sending E-mail")
        from_email = settings.SENDER_EMAIL_ADDRESS
        to = settings.AUDIT_TOOL_EMAIL_ADDRESSES
        bcc = []
        replay_to = ""

        content_type = "application" \
                       "/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        email = EmailMessage(
            subject=subject,
            body=body,
            from_email=from_email,
            to=to,
            bcc=bcc,
            reply_to=replay_to,
        )
        email.attach(filename, xlsx_data, content_type)
        email.send(fail_silently=False)

    def _get_account_info(self, data):
        info = {}
        for row in data:
            account_id = row.get("AccountId")
            campaign_name = row.get("CampaignName")
            impressions = int(row.get("Impressions"))

            if account_id not in info:
                info[account_id] = {}

            if campaign_name not in info[account_id]:
                info[account_id][campaign_name] = 0

            info[account_id][campaign_name] += impressions

        lines = []
        for account_id, campaign_info in info.items():
            for campaign_name, impressions in campaign_info.items():
                    line = "{}: {} ---> {}".format(
                        impressions,
                        self.accounts_dict[account_id],
                        campaign_name,
                    )
                    lines.append(line)
        return lines

    def save_s3(self, filename, data, content_type=None):
        s3 = boto3.client("s3",
                          aws_access_key_id=settings.AMAZON_S3_ACCESS_KEY_ID,
                          aws_secret_access_key=settings.AMAZON_S3_SECRET_ACCESS_KEY)

        s3.put_object(Bucket=settings.AMAZON_S3_BUCKET_NAME,
                      Key=self.s3_folder + "/" + filename,
                      Body=data,
                      ContentType)
        s3.close()
