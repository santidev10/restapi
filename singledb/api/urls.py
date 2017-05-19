from django.conf.urls import url

from singledb.api.views import CountryListApiView


urlpatterns = [
    url(r'^countries/$', CountryListApiView.as_view(), name="countries_list"),
]
