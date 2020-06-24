from rest_framework.status import HTTP_400_BAD_REQUEST

from saas.urls.namespaces import Namespace
from userprofile.api.urls.names import UserprofilePathName
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class ChangePasswordTestCase(ExtendedAPITestCase):
    change_password_url = reverse(
        UserprofilePathName.CHANGE_PASSWORD,
        [Namespace.USER_PROFILE]
    )

    def test_new_password_validation(self):
        self.create_test_user()
        bad_passwords = [
            "Short1!",
            "no_capitalization1!",
            "Nospecialchars1",
            "No_numbers!"
        ]
        for bad_password in bad_passwords:
            response = self.client.post(self.change_password_url, data={
                "new_password": bad_password,
            })
            self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
            self.assertIsNotNone(response.data.get("new_password", None))
