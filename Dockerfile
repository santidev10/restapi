FROM python:3.7.4 as base
ENV PYTHONUNBUFFERED 1
RUN pip install uwsgi
COPY ./requirements.txt /tmp/
COPY ./requirements.dev.txt /tmp/
COPY ./es_components/requirements.txt /tmp/requirements.es_componenets.txt
RUN pip install -r /tmp/requirements.txt
RUN pip install -r /tmp/requirements.dev.txt
RUN pip install -r /tmp/requirements.es_componenets.txt
WORKDIR /app
ENV PYTHONPATH=/app
RUN chown -R www-data:www-data /app

FROM base as prod
COPY --chown=www-data:www-data ./ /app
RUN python ./manage.py collectstatic --no-input
ARG APP_VERSION
ENV APP_VERSION=$APP_VERSION
USER www-data
CMD ["uwsgi", "--ini", "/app/uwsgi-restapi.ini"]

FROM base as dev
COPY ./requirements.dev.txt /tmp/
RUN pip install -r /tmp/requirements.dev.txt
EXPOSE 5000
VOLUME /app
COPY ./ /app
CMD ["python","./manage.py", "runserver", "0.0.0.0:5000"]
