FROM python:3.7.4 as base
ENV PYTHONUNBUFFERED 1
RUN pip install uwsgi
COPY ./requirements.txt /tmp/
COPY ./es_components/requirements.txt /tmp/requirements.es_componenets.txt
RUN pip install -r /tmp/requirements.txt
RUN pip install -r /tmp/requirements.es_componenets.txt
WORKDIR /app
ENV PYTHONPATH=/app
EXPOSE 5000

FROM base as prod
COPY --chown=www-data:www-data ./ /app
ARG APP_VERSION
ENV APP_VERSION=$APP_VERSION
USER www-data
CMD ["uwsgi", "--ini", "/app/uwsi-restapi.ini"]


FROM base as dev
COPY ./requirements.dev.txt /tmp/
RUN pip install -r /tmp/requirements.dev.txt
VOLUME /app
COPY ./ /app
CMD ["python","./manage.py", "runserver", "0.0.0.0:5000"]
