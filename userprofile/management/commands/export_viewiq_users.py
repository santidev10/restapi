"""
Command to export users from ViewIq
"""
import logging

import xlsxwriter
from django.contrib.auth import get_user_model
from django.core.management import BaseCommand
from django.utils import timezone

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    start_column = 0
    model = get_user_model()
    separation_symbol = "|"

    def handle(self, *args, **options):
        logger.info("Start export users")
        self.__prepare_workbook()
        self.__set_format_options()
        self.__export_users()
        self.workbook.close()
        logger.info("Export users procedure has been finished")

    def __prepare_workbook(self):
        filename = "userprofile/fixtures/users_export_{}.xlsx"
        filename = filename.format(timezone.now().strftime("%Y-%m-%dT%H:%M"))
        self.workbook = xlsxwriter.Workbook(filename)

    def __prepare_worksheet(self, name):
        worksheet = self.workbook.add_worksheet(name)
        headers = (
            (
                "Email",
                "First Name",
                "Last Name",
                "Access",
                "Date Joined",
                "Last Login",
                "Related Channels"
            ),
        )
        columns_width = {
            0: 40,
            1: 40,
            2: 40,
            3: 40,
            4: 15,
            5: 15,
            6: 40
        }
        for key, value in columns_width.items():
            worksheet.set_column(key, key, value)
        start_row = 0
        start_row = self.__write_rows(
            headers, start_row, worksheet, self.header_format)
        return worksheet, start_row

    def __set_format_options(self):
        header_format_options = {
            "bold": True,
            "align": "center",
            "bg_color": "#C0C0C0",
            "border": True,
        }
        self.header_format = self.workbook.add_format(header_format_options)

    def __write_rows(self, data, start_row, worksheet, style=None):
        for row in data:
            for column, value in enumerate(row):
                current_column = self.start_column + column
                worksheet.write(start_row, current_column, value, style)
            start_row += 1
        return start_row

    def __export_users(self):
        worksheet, start_row = self.__prepare_worksheet("UsersViewIQ")
        query = self.model.objects.all().order_by("email")
        data = [
            [
                obj.email,
                obj.first_name,
                obj.last_name,
                self.separation_symbol.join(
                    obj.groups.values_list("name", flat=True)),
                obj.date_joined.strftime("%Y-%m-%d %H:%M:%S"),
                obj.last_login.strftime("%Y-%m-%d %H:%M:%S"),
                self.separation_symbol.join(
                    obj.channels.values_list("channel_id", flat=True)),
            ]
            for obj in query]
        self.__write_rows(data, start_row, worksheet)
