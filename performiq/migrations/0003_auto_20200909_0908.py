# Generated by Django 3.0.4 on 2020-09-09 09:08

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('performiq', '0002_oauthaccount_email'),
    ]

    operations = [
        migrations.AlterField(
            model_name='oauthaccount',
            name='token',
            field=models.CharField(max_length=255, null=True),
        ),
    ]