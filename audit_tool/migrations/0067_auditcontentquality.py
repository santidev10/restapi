# Generated by Django 3.0.4 on 2020-07-15 18:32

from django.db import migrations, models

QUALITY_CHOICES = [
        (0, "Poor"),
        (1, "Average"),
        (2, "Premium"),
    ]


def create_items(apps, schema_editor):
    AuditContentQuality = apps.get_model("audit_tool", "AuditContentQuality")
    for id, quality in QUALITY_CHOICES:
        AuditContentQuality.objects.create(id=id, quality=quality)


def reverse_code(*_, **__):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ('audit_tool', '0066_auto_20200710_2222'),
    ]

    operations = [
        migrations.CreateModel(
            name='AuditContentQuality',
            fields=[
                ('id', models.IntegerField(choices=[(0, 'Poor'), (1, 'Average'), (2, 'Premium')], primary_key=True, serialize=False)),
                ('quality', models.CharField(max_length=15)),
            ],
        ),
        migrations.RunPython(create_items, reverse_code=reverse_code)
    ]
