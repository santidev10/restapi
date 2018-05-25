from django.http import HttpResponse
from django.shortcuts import get_object_or_404

from email_reports.models import SavedEmail


def email_report_web_view(_, pk):
    report = get_object_or_404(SavedEmail, pk=pk)
    return HttpResponse(report.html)
