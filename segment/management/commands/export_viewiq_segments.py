"""
Command to export non-private segments data from ViewIq
"""
import logging

import xlsxwriter
from django.core.management import BaseCommand
from django.utils import timezone

from segment.models import SegmentChannel, SegmentVideo, SegmentKeyword
from utils.lang import deep_getattr

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    start_column = 0
    segment_model = (SegmentChannel, SegmentVideo, SegmentKeyword)

    def add_arguments(self, parser):
        parser.add_argument(
            "--private",
            type=bool,
            default=False
        )

    def handle(self, *args, **options):
        logger.info("Start export segments procedure")
        is_private_export = options.get("private")
        self.__setup_headers(is_private_export)
        self.__prepare_workbook(is_private_export)
        self.__set_format_options()
        for model in self.segment_model:
            self.__export_segments(model, is_private_export)
        self.workbook.close()
        logger.info("Export segments procedure has been finished")

    def __setup_headers(self, is_private_export):
        self.headers = (("Title", "Related Ids", "Category"),)
        if is_private_export:
            self.headers = (
                ("Title", "Related Ids", "Category", "Owner Email"),)

    def __prepare_workbook(self, is_private_export):
        filename = "segment/fixtures/segments_export_{}.xlsx"
        if is_private_export:
            filename = "segment/fixtures/segments_private_export_{}.xlsx"
        filename = filename.format(timezone.now().strftime("%Y-%m-%dT%H:%M"))
        self.workbook = xlsxwriter.Workbook(filename)

    def __prepare_worksheet(self, name):
        worksheet = self.workbook.add_worksheet(name)
        columns_width = {
            0: 37,
            1: 50,
            2: 14,
            3: 25
        }
        for key, value in columns_width.items():
            worksheet.set_column(key, key, value)
        start_row = 0
        start_row = self.__write_rows(
            self.headers, start_row, worksheet, self.header_format)
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

    def __export_segments(self, model, is_private_export):
        worksheet, start_row = self.__prepare_worksheet(
            "{}Segments".format(model.segment_type.capitalize()))
        query = model.objects.exclude(category=model.PRIVATE)
        fields = ["title", "related_ids_string", "category"]
        if is_private_export:
            query = model.objects.filter(category=model.PRIVATE)
            fields += ["owner.email"]
        query = query.order_by("category")
        data = [[deep_getattr(obj, attr) for attr in fields] for obj in query]
        self.__write_rows(data, start_row, worksheet)
