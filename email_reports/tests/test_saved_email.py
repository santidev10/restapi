from django.core.urlresolvers import reverse
from django.test import TestCase
from email_reports.models import SavedEmail


class SavedEmailCase(TestCase):

    def test_get_not_found(self):
        url = reverse("email_reports_api_urls:email_report_web_view",
                      args=("32ab334",))

        response = self.client.get(url)
        self.assertEqual(response.status_code, 404)

    def test_get_saved_email(self):
        report = SavedEmail.objects.create(html="<h1>Hello, World!</h1>")

        url = reverse("email_reports_api_urls:email_report_web_view",
                      args=(report.id,))

        response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content.decode(), report.html)
