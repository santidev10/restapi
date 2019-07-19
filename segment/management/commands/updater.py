

from django.core.management import BaseCommand

from segment.models import CustomSegment



class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        s = CustomSegment.objects.get(id=9)
        s.update_statisatics()
