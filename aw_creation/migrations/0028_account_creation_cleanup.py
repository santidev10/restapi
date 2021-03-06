# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2018-08-21 11:48
from __future__ import unicode_literals

from django.db import migrations


def remove_extra_account_creations(apps, schema_editor):
    account_creation_model = apps.get_model('aw_creation.AccountCreation')
    account_creation_model.objects.filter(account__isnull=False, is_managed=False).delete()
    account_creation_model.objects.filter(account__isnull=True).delete()


class Migration(migrations.Migration):
    dependencies = [
        ('aw_creation', '0027_make_account_creation_owner_nullable'),
    ]

    operations = [
        migrations.RunPython(remove_extra_account_creations),
    ]
