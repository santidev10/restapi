from saas import celery_app
from performiq.models.models import OAuthAccount
from performiq.models.constants import OAuthType
from performiq.utils.dv360 import load_credentials
from performiq.utils.dv360 import get_discovery_resource

def update_partners():
    query = OAuthAccount.objects.filter(oauth_type=OAuthType.DV360.value)
    for account in query:
        credentials = load_credentials(account)
        resource = get_discovery_resource(credentials)
        # get partners
        # serialize
        # save partners
