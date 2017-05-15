from django.contrib.auth import get_user_model
from rest_framework.authtoken.models import Token
from rest_framework.test import APITestCase


class ExtendedAPITestCase(APITestCase):

    multi_db = True

    test_user_data = {
        "username": "TestUser",
        "first_name": "TestUser",
        "last_name": "TestUser",
        "email": "test@example.com",
        "password": "test"
    }

    def create_test_user(self, auth=True):
        """
        Make test user
        """
        user, created = get_user_model().objects.get_or_create(
            email=self.test_user_data["email"],
            defaults=self.test_user_data,
        )
        user.set_password(user.password)

        if auth:
            token = Token.objects.create(user=user)
            self.client.credentials(
                HTTP_AUTHORIZATION='Token {}'.format(token.key)
            )
        return user
