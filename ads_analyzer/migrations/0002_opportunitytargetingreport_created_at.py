# Generated by Django 2.2.4 on 2019-10-03 13:36

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ads_analyzer', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='opportunitytargetingreport',
            name='created_at',
            field=models.DateTimeField(auto_now_add=True, null=True),
        ),
    ]
