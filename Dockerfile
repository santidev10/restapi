FROM python:3.7
ENV PYTHONUNBUFFERED 1
RUN mkdir /code
RUN mkdir /code/es_components
WORKDIR /code
ADD requirements.txt /code/
ADD requirements.dev.txt /code/
ADD es_components/requirements.txt /code/es_components/requirements.txt
RUN pip install -r requirements.dev.txt
RUN pip install -r /code/es_components/requirements.txt
ADD . /code/
EXPOSE 5000
ADD https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh
CMD ["./manage.py", "runserver", "0.0.0.0:5000"]