"""
Command to export non-private segments data from ViewIq
"""
import logging

import xlsxwriter
from django.core.management import BaseCommand
from django.utils import timezone

from segment.models import SegmentChannel, SegmentVideo, SegmentKeyword

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    start_column = 0
    ids_separation_symbol = "|"
    segment_model = (SegmentChannel, SegmentVideo, SegmentKeyword)

    def handle(self, *args, **options):
        logger.info("Start export segments procedure")
        self.__prepare_workbook()
        self.__set_format_options()
        for model in self.segment_model:
            self.__export_segments(model)
        self.workbook.close()
        logger.info("Export segments procedure has been finished")

    def __prepare_workbook(self):
        filename = "segment/fixtures/segment_export_{}.xlsx".format(
            timezone.now().strftime("%Y-%m-%d"))
        self.workbook = xlsxwriter.Workbook(filename)

    def __prepare_worksheet(self, name):
        worksheet = self.workbook.add_worksheet(name)
        columns_width = {
            0: 37,
            1: 50,
            2: 14
        }
        for key, value in columns_width.items():
            worksheet.set_column(key, key, value)
        headers = (("Title", "Related Ids", "Category"),)
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

    def __export_segments(self, model):
        worksheet, start_row = self.__prepare_worksheet(
            "{}Segments".format(model.segment_type.capitalize()))
        query = model.objects.exclude(
            category=SegmentChannel.PRIVATE).order_by("category")
        data = [(obj.title, self.ids_separation_symbol.join(
                 obj.related_ids_list), obj.category)
                for obj in query]
        self.__write_rows(data, start_row, worksheet)
