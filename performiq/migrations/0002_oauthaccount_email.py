# Generated by Django 3.0.4 on 2020-09-03 07:34

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('performiq', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='oauthaccount',
            name='email',
            field=models.EmailField(db_index=True, max_length=255, null=True),
        ),
    ]
