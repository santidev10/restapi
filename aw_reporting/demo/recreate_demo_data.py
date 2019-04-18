from saas import celery_app


@celery_app.task()
def recreate_demo_data():
    pass
