FROM python:3.5
ENV PYTHONUNBUFFERED 1
RUN mkdir /code
WORKDIR /code
ADD requirements.txt /code/
ADD requirements.dev.txt /code/
RUN pip install -r requirements.txt
RUN pip install -r requirements.dev.txt
ADD . /code/
EXPOSE 5000
ADD https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh
CMD ["./start.sh"]