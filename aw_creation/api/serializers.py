from aw_creation.models import *
from rest_framework.serializers import ModelSerializer, \
    SerializerMethodField, FloatField, IntegerField, DateField, \
    PrimaryKeyRelatedField, CharField, StringRelatedField, ListField, \
    ValidationError, BooleanField, Serializer
from django.contrib.auth import get_user_model
from collections import defaultdict
from django.db.models import QuerySet, Max, Value, F, Case, When, Sum,\
    IntegerField as AggrIntegerField, FloatField as AggrFloatField, \
    ExpressionWrapper
from datetime import datetime
import pytz
import math
import json
import re


class OptimizationAccountListSerializer(ModelSerializer):
    is_optimization_active = SerializerMethodField()

    def __init__(self):
        super(OptimizationAccountListSerializer, self).__init__()
        self.today = datetime.now()

    class Meta:
        model = AccountCreation
        fields = (
            "id", "name", "is_ended", "is_paused",
            "is_optimization_active", "is_changed", "is_approved",
            # from the campaigns
            "start", "end",
            # plan stats
            "ordered_cpm", "ordered_cpv", "ordered_impressions",
            "ordered_impressions_cost", "ordered_views",
            "ordered_views_cost",
            # delivered stats
            "cpm", "cpv", "impressions",
            "impressions_cost", "views",
            "views_cost",
        )

    @staticmethod
    def get_is_optimization_active(*_):
        return True
