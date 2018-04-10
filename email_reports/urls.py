from django.conf.urls import url
from .views import email_report_web_view

urlpatterns = [
    url(r'^email_report_web_view/(?P<pk>\w+)/$',
        email_report_web_view, name='email_report_web_view'),
]
