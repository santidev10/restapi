"""
Administration models module
"""
from django.db.models import CASCADE
from django.db.models import CharField
from django.db.models import DateTimeField
from django.db.models import ForeignKey
from django.db.models import Model
from django.db.models import URLField


class UserAction(Model):
    """
    Model to store user actions (page visits, etc.)
    """
    user = ForeignKey('userprofile.userprofile', null=True, blank=True, on_delete=CASCADE)
    slug = CharField(max_length=20, null=True, blank=True)
    url = URLField(max_length=200, null=True, blank=True)
    created_at = DateTimeField(auto_now_add=True)
