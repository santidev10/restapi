FROM python:3.9.1 as base
ENV PYTHONUNBUFFERED 1
RUN pip install --upgrade pip==21.0.1
ENV ELASTIC_SEARCH_URLS es
COPY ./requirements.txt /tmp/
COPY ./es_components/requirements.txt /tmp/requirements.es_componenets.txt
RUN pip install -r /tmp/requirements.txt
RUN pip install -r /tmp/requirements.es_componenets.txt

# Install pycld2 in the container
RUN git clone https://github.com/aboSamoor/pycld2 /tmp/pycld2
RUN sed -i "s/\"-m64\",//g" /tmp/pycld2/setup.py
WORKDIR /tmp/pycld2/
RUN python /tmp/pycld2/setup.py install

WORKDIR /app
ENV PYTHONPATH=/app
RUN chown -R www-data:www-data /app

FROM base as prod
COPY --chown=www-data:www-data ./ /app
RUN python ./manage.py collectstatic --no-input
ARG APP_VERSION
ENV APP_VERSION=$APP_VERSION
RUN pip install uwsgi==2.0.19.1
USER www-data
CMD ["uwsgi", "--ini", "/app/uwsgi-restapi.ini"]

FROM base as dev
COPY ./requirements.dev.txt /tmp/
RUN pip install -r /tmp/requirements.dev.txt
EXPOSE 5000
VOLUME /app
COPY ./ /app
CMD ["python","./manage.py", "runserver", "0.0.0.0:5000"]
