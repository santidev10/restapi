# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2018-08-21 11:54
from __future__ import unicode_literals

from django.db import migrations


def add_missing_account_creations(apps, schema_editor):
    account_creation_model = apps.get_model('aw_creation.AccountCreation')
    account_model = apps.get_model('aw_reporting.Account')
    account_with_no_account_creation = account_model.objects \
        .filter(account_creation__isnull=True)
    bulk_create = [
        account_creation_model(account=account, is_managed=False, owner=None)
        for account in account_with_no_account_creation
    ]
    account_creation_model.objects.bulk_create(bulk_create)


class Migration(migrations.Migration):
    dependencies = [
        ('aw_creation', '0029_account_creation_ono_to_one_to_account'),
    ]

    operations = [
        migrations.RunPython(add_missing_account_creations),
    ]