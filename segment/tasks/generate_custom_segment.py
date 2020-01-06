import logging

from django.conf import settings
from django.utils import timezone

from administration.notifications import send_html_email
from saas import celery_app
from segment.models import CustomSegment
from segment.tasks.generate_segment import generate_segment

logger = logging.getLogger(__name__)


@celery_app.task
def generate_custom_segment(segment_id):
    try:
        segment = CustomSegment.objects.get(id=segment_id)
        export = segment.export
        results = generate_segment(segment, export.query["body"], segment.LIST_SIZE)
        segment.statistics = results["statistics"]
        export.download_url = results["download_url"]
        export.completed_at = timezone.now()
        export.save()
        segment.save()
        export.refresh_from_db()
        subject = "Custom Target List: {}".format(segment.title)
        text_header = "Your Custom Target List {} is ready".format(segment.title)
        text_content = "<a href={download_url}>Click here to download</a>".format(download_url=export.download_url)
        send_html_email(
            subject=subject,
            to=segment.owner.email,
            text_header=text_header,
            text_content=text_content,
            from_email=settings.EXPORTS_EMAIL_ADDRESS
        )
        logger.info(f"Successfully generated export for custom list: id: {segment.id}, title: {segment.title}")
    except Exception as e:
        logger.error(f"Error in generate_custom_segment task:\n{e}")
