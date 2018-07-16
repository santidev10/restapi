from aw_reporting.api.constants import DashboardRequest


def is_dashboard_request(request):
    key = DashboardRequest.DASHBOARD_PARAM_NAME
    expected_value = DashboardRequest.DASHBOARD_PARAM_VALUE
    query_param = request.query_params.get(key)
    body_param = request.body.get(key)
    return query_param == str(expected_value) or body_param == expected_value
