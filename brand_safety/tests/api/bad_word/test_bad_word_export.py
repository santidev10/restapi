from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN

from audit_tool.models import AuditLanguage
from brand_safety.api.urls.names import BrandSafetyPathName as PathNames
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from saas.urls.namespaces import Namespace
from utils.unittests.csv import get_data_from_csv_response
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class BadWordExportTestCase(ExtendedAPITestCase):
    def _request(self):
        url = reverse(
            PathNames.BadWord.EXPORT,
            [Namespace.BRAND_SAFETY],
        )
        return self.client.get(url)

    def test_not_auth(self):
        response = self._request()

        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_no_permissions(self):
        self.create_test_user()

        response = self._request()

        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_success(self):
        self.create_admin_user()

        response = self._request()

        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(response["Content-Type"], "text/csv")
        expected_filename = "Bad Words.csv"
        self.assertEqual(response["Content-Disposition"], "attachment; filename=\"{}\"".format(expected_filename))

    def test_response(self):
        self.create_admin_user()
        category = BadWordCategory.objects.create(name="test category")
        language = AuditLanguage.objects.create(language="sv")
        bad_word = BadWord.objects.create(
            id=next(int_iterator),
            name="test name",
            category=category,
            language=language
        )

        response = self._request()

        csv_data = get_data_from_csv_response(response)
        headers = next(csv_data)
        self.assertEqual(headers, [
            "Name",
            "Category",
            "Language",
            "Score"
        ])
        data = next(csv_data)
        self.assertEqual(data, [bad_word.name, bad_word.category.name,
                                bad_word.language.language, str(bad_word.negative_score)])
