"""
Administration api urls module
"""
from django.conf.urls import url

from administration.api.views import UserListAdminApiView, \
    UserRetrieveUpdateDeleteAdminApiView, AuthAsAUserAdminApiView, \
    UserActionListCreateApiView, UserActionDeleteAdminApiView, \
    PlanListCreateApiView, PlanChangeDeleteApiView, SubscriptionView, \
    SubscriptionCreateView

urlpatterns = [
    url(r'^users/$', UserListAdminApiView.as_view(), name="user_list"),
    url(r'^users/(?P<pk>\d+)/$', UserRetrieveUpdateDeleteAdminApiView.as_view(),
        name="user_details"),
    url(r'^users/(?P<pk>\d+)/auth/$', AuthAsAUserAdminApiView.as_view(),
        name="user_auth_admin"),
    url(r'^user_actions/$', UserActionListCreateApiView.as_view(),
        name="user_action_list"),
    url(r'^user_actions/(?P<pk>\d+)/$', UserActionDeleteAdminApiView.as_view(),
        name="user_action_details"),
    url(r'^plan/(?P<pk>)/$', PlanChangeDeleteApiView.as_view(), name="plan"),
    url(r'^plan/$', PlanListCreateApiView.as_view(), name="plan"),

    url(r"^subscriptions/$", SubscriptionView.as_view(), name="subscription_list"),
    url(r"^subscriptions/create/$", SubscriptionCreateView.as_view(), name="subscription_create"),
    # url(r"^subscriptions/(?P<pk>\d+)/delete/$", SubscriptionDeleteView.as_view(), name="subscription_delete"),
    # url(r"^subscriptions/(?P<pk>\d+)/update/$", SubscriptionUpdateView.as_view(), name="subscription_update"),
]
