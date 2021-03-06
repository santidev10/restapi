"""
Utils models module
"""
from django.db.models import DateTimeField
from django.db.models import Model


class Timestampable(Model):
    """
    Create and update instance time
    """
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    class Meta:
        abstract = True
