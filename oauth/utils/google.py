def get_aw_customers(refresh_token):
    config = load_client_settings()
    aw_client = get_client(
        client_customer_id=None,
        refresh_token=refresh_token,
        **config
    )
    customer_service = aw_client.GetService(
        "CustomerService", version=API_VERSION
    )
    return customer_service.getCustomers()