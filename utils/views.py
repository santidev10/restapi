from django.http import HttpResponse

XLSX_CONTENT_TYPE = "application/" \
                    "vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def xlsx_response(title, xlsx):
    response = HttpResponse(
        xlsx,
        content_type=XLSX_CONTENT_TYPE
    )
    response["Content-Disposition"] = "attachment; filename=\"{}.xlsx\"".format(
        title)
    return response
