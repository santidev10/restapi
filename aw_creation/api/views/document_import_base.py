import csv

from openpyxl import load_workbook
from rest_framework.generics import GenericAPIView
from rest_framework.parsers import FileUploadParser

DOCUMENT_LOAD_ERROR_TEXT = "Only Microsoft Excel(.xlsx) and CSV(.csv) files are allowed. " \
                           "Also please use the UTF-8 encoding. It is expected that item ID " \
                           "placed in one of first two columns."


class DocumentImportBaseAPIView(GenericAPIView):
    parser_classes = (FileUploadParser,)

    @staticmethod
    def get_xlsx_contents(file_obj, return_lines=False):
        wb = load_workbook(file_obj)
        for sheet in wb:
            for row in sheet:
                if return_lines:
                    yield tuple(i.value for i in row)
                else:
                    for cell in row:
                        yield cell.value

    @staticmethod
    def get_csv_contents(file_obj, return_lines=False):
        string = file_obj.read().decode()
        reader = csv.reader(string.split("\n"))
        for row in reader:
            if return_lines:
                yield tuple(i.strip() for i in row)
            else:
                for cell in row:
                    yield cell.strip()
