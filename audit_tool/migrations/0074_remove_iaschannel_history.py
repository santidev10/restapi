# Generated by Django 3.0.4 on 2020-10-16 21:13

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('audit_tool', '0073_auto_20201012_1113'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='iaschannel',
            name='history',
        ),
    ]
