FROM python:3.7 as base
WORKDIR /usr/lib/uwsgi/plugins/
RUN apt update && \
	apt install -y \
		uwsgi \
		uwsgi-src \
		libcap-dev && \
	uwsgi --build-plugin \
		"/usr/src/uwsgi/plugins/python python37" && \
	chmod 644 python37_plugin.so
ENV PYTHONUNBUFFERED 1
COPY ./requirements.txt /tmp/
COPY ./es_components/requirements.txt /tmp/requirements.es_componenets.txt
COPY ./uwsgi-restapi.ini /etc/uwsgi/restapi.ini
RUN pip install -r /tmp/requirements.txt
RUN pip install -r /tmp/requirements.es_componenets.txt
WORKDIR /app
RUN chown -R www-data:www-data /app
USER www-data
EXPOSE 5000
CMD ["python","./manage.py", "runserver", "0.0.0.0:5000"]

FROM base as prod
COPY --chown=www-data:www-data ./ /app


FROM base as dev
ENV PYTHONPATH=/app
COPY ./requirements.dev.txt /tmp/
USER root
RUN pip install -r /tmp/requirements.dev.txt
COPY --chown=www-data:www-data ./ /app
USER www-data
