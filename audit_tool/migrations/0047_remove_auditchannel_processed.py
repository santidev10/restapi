# Generated by Django 2.2.4 on 2019-12-11 19:08

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ('audit_tool', '0046_auditcategory_category_display_iab'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='auditchannel',
            name='processed',
        ),
    ]
