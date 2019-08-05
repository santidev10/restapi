from django.conf.urls import url

from channel.api.country_view import CountryListApiView
from singledb.api.views.audit import AuditKeyWordsExportApiView
from singledb.api.views.bad_words import urls as bad_words_legacy_urls

urlpatterns = [
    url(r"^countries/$", CountryListApiView.as_view(), name="countries_list"),
    url(r"^audit/keywords/export/$", AuditKeyWordsExportApiView.as_view(), name="audit_keywords_export"),
    *bad_words_legacy_urls,  # fixme: old bad words. remove it
]
