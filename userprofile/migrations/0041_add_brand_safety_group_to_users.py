# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2019-05-28 19:38
from __future__ import unicode_literals

from django.db import migrations


def add_brand_safety_group_to_users(apps, schema_editor):
    Group = apps.get_model("auth", "Group")
    try:
        brand_safety_group = Group.objects.get(name="Brand Safety Scoring")
        UserProfile = apps.get_model("userprofile", "UserProfile")
        for user in UserProfile.objects.all():
            user.groups.add(brand_safety_group)
    except Exception:
        pass


def remove_brand_safety_group_from_users(apps, schema_editor):
    Group = apps.get_model("auth", "Group")
    try:
        brand_safety_group = Group.objects.get(name="Brand Safety Scoring")
        UserProfile = apps.get_model("userprofile", "UserProfile")
        for user in UserProfile.objects.all():
            user.groups.remove(brand_safety_group)
    except Exception:
        pass


class Migration(migrations.Migration):

    dependencies = [
        ('userprofile', '0040_auto_20190516_1912'),
    ]

    operations = [
        migrations.RunPython(add_brand_safety_group_to_users, remove_brand_safety_group_from_users),
    ]
