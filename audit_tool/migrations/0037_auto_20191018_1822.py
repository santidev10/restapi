# Generated by Django 2.2.4 on 2019-10-18 18:22

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('audit_tool', '0036_auto_20190930_2156'),
    ]

    operations = [
        migrations.AlterField(
            model_name='auditvideotranscript',
            name='language',
            field=models.ForeignKey(default=None, null=True, on_delete=django.db.models.deletion.CASCADE, to='audit_tool.AuditLanguage'),
        ),
    ]
