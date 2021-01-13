# Generated by Django 3.0.4 on 2021-01-13 23:14

from django.conf import settings
import django.contrib.postgres.fields.jsonb
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='FTUX',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('feature', models.CharField(max_length=10, unique=True)),
                ('refresh_time', models.DateTimeField(db_index=True, default=None, null=True)),
                ('meta_data', django.contrib.postgres.fields.jsonb.JSONField(default=dict)),
            ],
        ),
        migrations.CreateModel(
            name='FTUXUser',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('last_seen', models.DateTimeField(db_index=True, default=None, null=True)),
                ('ftux', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='ftux.FTUX')),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'unique_together': {('user', 'ftux')},
            },
        ),
    ]
