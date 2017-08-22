"""
Administration models module
"""
from django.db.models import Model, URLField, CharField, DateTimeField,\
    ForeignKey


class UserAction(Model):
    """
    Model to store user actions (page visits, etc.)
    """
    user = ForeignKey('userprofile.userprofile', null=True, blank=True)
    slug = CharField(max_length=20, null=True, blank=True)
    url = URLField(max_length=200, null=True, blank=True)
    created_at = DateTimeField(auto_now_add=True)
