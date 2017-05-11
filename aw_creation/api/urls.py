from django.conf.urls import url
from aw_creation.api import views

urlpatterns = [
    url(r'^optimization_account_list/$',
        views.OptimizationAccountListApiView.as_view(),
        name="optimization_account_list"),
]