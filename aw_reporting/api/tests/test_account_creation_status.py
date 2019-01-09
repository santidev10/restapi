from django.utils import timezone
from rest_framework.test import APITestCase

from aw_creation.models import AccountCreation
from aw_reporting.models import Account
from aw_reporting.models import Campaign


class AnalyticsAccountCreationListAPITestCase(APITestCase):
    def test_account_creation_status_ended(self):
        account = Account.objects.create()
        account_creation = account.account_creation
        Campaign.objects.create(account=account, status="ended")
        self.assertEqual(AccountCreation.STATUS_ENDED, account_creation.status)
    
    def test_account_creation_status_paused(self):
        account = Account.objects.create()
        account_creation = account.account_creation
        Campaign.objects.create(id="1", account=account, status="paused")
        Campaign.objects.create(id="2", account=account, status="ended")
        Campaign.objects.create(id="3", account=account, status="removed")
        Campaign.objects.create(id="4", account=account, status="suspended")
        self.assertEqual(AccountCreation.STATUS_PAUSED, account_creation.status)

    def test_account_creation_status_running(self):
        account = Account.objects.create()
        account_creation = account.account_creation
        account_creation.sync_at = timezone.now()
        account_creation.save()
        self.assertEqual(AccountCreation.STATUS_RUNNING, account_creation.status)
        account_creation.sync_at = None
        account_creation.is_managed = False
        account_creation.save()
        self.assertEqual(AccountCreation.STATUS_RUNNING, account_creation.status)

    def test_account_creation_status_pending(self):
        account = Account.objects.create()
        account_creation = account.account_creation
        account_creation.is_approved = True
        account_creation.is_managed = True
        account_creation.save()
        self.assertEqual(AccountCreation.STATUS_PENDING, account_creation.status)

    def test_account_creation_status_draft(self):
        account = Account.objects.create()
        account_creation = account.account_creation
        account_creation.is_managed = True
        self.assertEqual(AccountCreation.STATUS_DRAFT, account.account_creation.status)
