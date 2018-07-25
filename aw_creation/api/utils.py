from aw_reporting.api.constants import DashboardRequest


def _get_body_dict(request):
    return request.data if hasattr(request, "data") else dict()


def is_dashboard_request(request):
    key = DashboardRequest.DASHBOARD_PARAM_NAME
    expected_value = DashboardRequest.DASHBOARD_PARAM_VALUE
    query_param = request.query_params.get(key)
    body_param = _get_body_dict(request).get(key)
    return query_param == str(expected_value) or body_param == expected_value
