from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_401_UNAUTHORIZED, HTTP_200_OK

from aw_reporting.models import UserRole, User, Opportunity, Category
from utils.utils_tests import ExtendedAPITestCase


class PacingReportOpportunitiesTestCase(ExtendedAPITestCase):
    def setUp(self):
        self.url = reverse("aw_reporting_urls:pacing_report_filters")

    def test_get_filters_no_permission(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_401_UNAUTHORIZED)

    def test_get_filters(self):
        """
        we gonna check that all the filters are available, simple check lengths
        and carefully check user roles (am, sales, ad_ops)
        :return:
        """
        self.create_test_user()
        for uid, name in (("1", UserRole.AD_OPS_NAME),
                          ("2", UserRole.ACCOUNT_MANAGER_NAME),
                          ("3", "Super-sales-man")):
            UserRole.objects.create(id=uid, name=name)

        users = (
            ("Jah", "1", True),
            ("Tal", "1", False),
            ("Dol", "2", False),
            ("Ith", "2", True),
            ("Sol", "2", False),
            ("Ral", "3", True),
            ("Um", "3", False),
            ("Ber", None, True),
        )
        User.objects.bulk_create(
            [User(id=name, name=name, role_id=role_id, is_active=is_active)
             for name, role_id, is_active in users])
        Opportunity.objects.create(id=1, name="1", probability=100, )
        Opportunity.objects.create(id=2, name="2", probability=100, )
        Opportunity.objects.create(id=3, name="3", probability=100, )

        response = self.client.get(self.url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data
        self.assertEqual(
            set(data.keys()),
            {
                'start', 'end', 'status', 'ad_ops', 'am', 'sales',
                'goal_type', 'period', 'category', 'region',
            }
        )
        self.assertEqual(len(data['period']), 7)
        self.assertEqual(len(data['status']), 3)

        self.assertEqual(
            set(u['id'] for u in data['ad_ops']), {"Jah"}
        )
        self.assertEqual(
            set(u['id'] for u in data['am']), {"Ith"}
        )
        self.assertEqual(
            set(u['id'] for u in data['sales']), {"Ral"}
        )

    def test_category(self):
        self.create_test_user()
        categories = [
            Category.objects.create(id="1"),
            Category.objects.create(id="2"),
            Category.objects.create(id="3"),
        ]
        category_ids = sorted([c.id for c in categories])

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)

        categories_data = response.data["category"]
        categories_data_ids = [c["id"] for c in categories_data]
        self.assertEqual(categories_data_ids, category_ids)
