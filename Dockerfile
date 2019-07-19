FROM python:3.7 as prod
ENV PYTHONUNBUFFERED 1
COPY ./requirements.txt /tmp/
COPY ./es_components/requirements.txt /tmp/requirements.es_componenets.txt
RUN pip install -r /tmp/requirements.txt
RUN pip install -r /tmp/requirements.es_componenets.txt
COPY --chown=www-data:www-data ./ /app
WORKDIR /app
USER www-data
EXPOSE 5000
CMD ["python","./manage.py", "runserver", "0.0.0.0:5000"]

FROM prod as dev
ENV PYTHONPATH=/app
COPY ./requirements.dev.txt /tmp/
USER root
ADD https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh /wait-for-it.sh
RUN chmod +rx /wait-for-it.sh
RUN pip install -r /tmp/requirements.dev.txt
USER www-data
