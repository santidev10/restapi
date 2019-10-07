# Generated by Django 2.2.4 on 2019-09-30 21:56

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('audit_tool', '0035_auditvideotranscript'),
    ]

    operations = [
        migrations.AddField(
            model_name='auditvideotranscript',
            name='language',
            field=models.ForeignKey(default=1, on_delete=django.db.models.deletion.CASCADE, to='audit_tool.AuditLanguage'),
        ),
        migrations.AlterUniqueTogether(
            name='auditvideotranscript',
            unique_together={('video', 'language')},
        ),
    ]
