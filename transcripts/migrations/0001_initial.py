# Generated by Django 2.2.4 on 2020-01-31 23:45

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('audit_tool', '0049_auditvideo_processed_time'),
    ]

    operations = [
        migrations.CreateModel(
            name='SQTranscript',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('video_id', models.CharField(max_length=50, unique=True)),
                ('video_id_hash', models.BigIntegerField(db_index=True, default=0)),
                ('transcript', models.TextField(blank=True, default=None, null=True)),
                ('submitted', models.DateTimeField(blank=True, default=None, null=True)),
                ('retrieved', models.DateTimeField(blank=True, default=None, null=True)),
                ('job_id', models.CharField(blank=True, default=None, max_length=255, null=True)),
                ('language', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='transcript_language', to='audit_tool.AuditLanguage')),
            ],
            options={
                'abstract': False,
            },
        ),
    ]
