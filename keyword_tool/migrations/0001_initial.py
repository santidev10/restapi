# -*- coding: utf-8 -*-
# Generated by Django 1.9.9 on 2017-02-12 20:45
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Collaborator',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('email', models.EmailField(db_index=True, max_length=254)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='Interest',
            fields=[
                ('id', models.IntegerField(primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=200)),
            ],
        ),
        migrations.CreateModel(
            name='KeyWord',
            fields=[
                ('text', models.CharField(max_length=250, primary_key=True, serialize=False)),
                ('average_cpc', models.FloatField(null=True)),
                ('competition', models.FloatField(null=True)),
                ('_monthly_searches', models.TextField(null=True)),
                ('search_volume', models.IntegerField(null=True)),
                ('interests', models.ManyToManyField(to='keyword_tool.Interest')),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='KeywordsList',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.TextField()),
                ('user_email', models.EmailField(db_index=True, max_length=254)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('visible_for_all', models.BooleanField(db_index=True, default=False)),
                ('collaborators', models.ManyToManyField(related_name='collaborative_lists', to='keyword_tool.Collaborator')),
                ('keywords', models.ManyToManyField(related_name='lists', to='keyword_tool.KeyWord')),
            ],
            options={
                'ordering': ['-updated_at'],
            },
        ),
        migrations.CreateModel(
            name='Query',
            fields=[
                ('text', models.CharField(max_length=250, primary_key=True, serialize=False)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
        ),
        migrations.AddField(
            model_name='keyword',
            name='queries',
            field=models.ManyToManyField(related_name='keywords', to='keyword_tool.Query'),
        ),
    ]
