# Generated by Django 2.2.4 on 2019-12-04 20:03

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('audit_tool', '0045_auditchannelmeta_monetised'),
    ]

    operations = [
        migrations.AddField(
            model_name='auditcategory',
            name='category_display_iab',
            field=models.TextField(default=None, null=True),
        ),
    ]
