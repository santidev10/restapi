"""
Feedback api urls module
"""
from django.conf.urls import url

from feedback.api.views import FeedbackRetrieveUpdateApiView,\
    FeedbackSendApiView

urlpatterns = [
    url(r'^contacts/$', ContactsApiView.as_view(), name="contacts"),
]
