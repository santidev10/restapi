from aw_reporting.adwords_reports import account_performance_report
from aw_reporting.adwords_api import get_web_app_client, get_all_customers
from suds import WebFault


def check_users_to_aw_accounts_permissions():
    from aw_reporting.models import AWAccountPermission, Account

    for perm in AWAccountPermission.objects.all():
        client = get_web_app_client(
            refresh_token=perm.aw_connection.refresh_token,
            client_customer_id=perm.account_id,
        )
        try:
            accounts = list(filter(lambda i: not i['canManageClients'],
                            get_all_customers(client)))
        except WebFault as e:
            if "AuthorizationError.USER_PERMISSION_DENIED" in\
                    e.fault.faultstring:
                # access was terminated
                perm.can_read = False
            else:
                raise
        else:
            perm.can_read = True
            print(accounts)
            # also we can update management accounts
            save_advertise_accounts(accounts, perm.account)

            # now lets check write permission
            # TODO: this works but shouldn't, find another solution
            managed_customer_service = client.GetService(
                  'ManagedCustomerService',
                   version='v201607'
            )
            # Construct operations and add campaign.
            operations = [{
                'operator': 'ADD',
                'operand': {
                    'name': 'Check permission account',
                    'currencyCode': 'EUR',
                    'dateTimeZone': 'Europe/London',
                }
            }]
            accounts = managed_customer_service.mutate(operations)
            print(accounts)

        finally:
            perm.save()


def save_advertise_accounts(accounts, manager):
    from aw_reporting.models import Account
    for e in accounts:
        Account.objects.update_or_create(
            id=e['customerId'],
            defaults=dict(
                name=e['name'],
                currency_code=e['currencyCode'],
                timezone=e['dateTimeZone'],
                manager=manager,
            )
        )



