from datetime import timedelta
from django.db.models import Q
from django.utils import timezone

from performiq.models.constants import OAuthType
from performiq.models.models import OAuthAccount
from performiq.tasks.dv360.serializers.partner_serializer import PartnerSerializer
from performiq.utils.dv360 import get_discovery_resource
from performiq.utils.dv360 import load_credentials


UPDATED_THRESHOLD_MINUTES = 30
CREATED_THRESHOLD_MINUTES = 2


def update_partners():
    """
    Updates partners for accounts that were either created
    recently, or have not been recently updated
    """
    created_threshold = timezone.now() - timedelta(minutes=CREATED_THRESHOLD_MINUTES)
    updated_threshold = timezone.now() - timedelta(minutes=UPDATED_THRESHOLD_MINUTES)
    query = OAuthAccount.objects.filter(
        Q(oauth_type=OAuthType.DV360.value) &
        (Q(created_at__gte=created_threshold) | Q(updated_at__lte=updated_threshold))
    )
    for account in query:
        credentials = load_credentials(account)
        resource = get_discovery_resource(credentials)
        partners_response = resource.partners().list().execute()
        items = partners_response["partners"]
        partners = []
        for item in items:
            serializer = PartnerSerializer(data=item)
            if serializer.is_valid():
                partner = serializer.save()
                partners.append(partner)

        account.dv360_partners.set(partners)
        account.updated_at = timezone.now()
        account.save(update_fields=["updated_at"])