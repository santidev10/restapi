from django.db import migrations


def add_managed_service_hide_group(apps, schema_editor):
    Group = apps.get_model("auth", "Group")
    try:
        delivery_data_group = Group(name="Hide in Managed Service - Delivery Data")
        delivery_data_group.save()

        performance_graph_group = \
            Group.objects.get(name="Hide Managed Service Performance Details")
        performance_graph_group.name = \
            "Hide in Managed Service - Performance Graph Section"
        performance_graph_group.save()
    except Group.DoesNotExist:
        pass


def remove_managed_service_hide_group(apps, schema_editor):
    Group = apps.get_model("auth", "Group")
    try:
        performance_graph_group = \
            Group.objects.get(name="Hide in Managed Service - Performance Graph Section")
        performance_graph_group.name = \
            "Hide Managed Service Performance Details"
        performance_graph_group.save()

        delivery_data_group = \
            Group.objects.get(name="Hide in Managed Service - Delivery Data")
        UserProfile = apps.get_model("userprofile", "UserProfile")
        for user in UserProfile.objects.all():
            user.groups.remove(delivery_data_group)
        delivery_data_group.delete()
    except Group.DoesNotExist:
        pass


class Migration(migrations.Migration):

    dependencies = [
        ('userprofile', '0047_domain_datamigration'),
    ]

    operations = [
        migrations.RunPython(add_managed_service_hide_group, remove_managed_service_hide_group),
    ]
