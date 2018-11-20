from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_401_UNAUTHORIZED, HTTP_200_OK

from aw_reporting.models import UserRole, User, Opportunity, Category
from utils.utittests.test_case import ExtendedAPITestCase


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

        am = User.objects.create(id="Ith", is_active=True)
        ad_ops = User.objects.create(id="Jah", is_active=True)
        sales = User.objects.create(id="Ral", is_active=True)
        users_data = dict(account_manager=am,
                          sales_manager=sales,
                          ad_ops_manager=ad_ops)
        Opportunity.objects.create(id=1, name="1", probability=100,
                                   **users_data)
        Opportunity.objects.create(id=2, name="2", probability=100,
                                   **users_data)
        Opportunity.objects.create(id=3, name="3", probability=100,
                                   **users_data)

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

    def test_sales(self):
        self.create_test_user()
        test_user = User.objects.create(id="1",
                                        name="Test User",
                                        is_active=True)
        User.objects.create(id="2", name="Test User 2", is_active=True)
        Opportunity.objects.create(id=1, sales_manager=test_user)
        Opportunity.objects.create(id=2, sales_manager=test_user)

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        sales_filter = response.data["sales"]
        expected_sales = [dict(id=test_user.id, name=test_user.name)]
        self.assertEqual(sales_filter, expected_sales)
        self.assertEqual(len(response.data["am"]), 0)
        self.assertEqual(len(response.data["ad_ops"]), 0)

    def test_account_managers(self):
        self.create_test_user()
        test_user = User.objects.create(id="1",
                                        name="Test User",
                                        is_active=True)
        User.objects.create(id="2", name="Test User 2", is_active=True)
        Opportunity.objects.create(id=1, account_manager=test_user)
        Opportunity.objects.create(id=2, account_manager=test_user)

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        am_filter = response.data["am"]
        expected_am = [dict(id=test_user.id, name=test_user.name)]
        self.assertEqual(am_filter, expected_am)
        self.assertEqual(len(response.data["sales"]), 0)
        self.assertEqual(len(response.data["ad_ops"]), 0)

    def test_ad_ops(self):
        self.create_test_user()
        test_user = User.objects.create(id="1",
                                        name="Test User",
                                        is_active=True)
        User.objects.create(id="2", name="Test User 2", is_active=True)
        Opportunity.objects.create(id=1, ad_ops_manager=test_user)
        Opportunity.objects.create(id=2, ad_ops_manager=test_user)

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        ad_ops_filter = response.data["ad_ops"]
        expected_ad_ops = [dict(id=test_user.id, name=test_user.name)]
        self.assertEqual(ad_ops_filter, expected_ad_ops)
        self.assertEqual(len(response.data["sales"]), 0)
        self.assertEqual(len(response.data["am"]), 0)
