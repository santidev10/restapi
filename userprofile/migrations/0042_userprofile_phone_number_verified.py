# Generated by Django 2.2.4 on 2020-01-28 19:35

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('userprofile', '0041_add_brand_safety_group_to_users'),
    ]

    operations = [
        migrations.AddField(
            model_name='userprofile',
            name='phone_number_verified',
            field=models.BooleanField(default=False),
        ),
    ]
