# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-03-13 14:13
from __future__ import unicode_literals

import csv

from django.db import migrations


def load_languages(apps, schema_editor):
    lang_model = apps.get_model('aw_creation.Language')
    with open('aw_creation/fixtures/languagecodes.csv') as f:
        reader = csv.reader(f)
        next(reader, None)  # title
        bulk = []
        for name, code, uid in reader:
            bulk.append(lang_model(id=uid, name=name, code=code))
        lang_model.objects.bulk_create(bulk)


class Migration(migrations.Migration):

    dependencies = [
        ('aw_creation', '0001_initial'),
    ]

    operations = [
        migrations.RunPython(load_languages),
    ]
