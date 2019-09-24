FROM docker.elastic.co/logstash/logstash:6.4.2
RUN logstash-plugin install logstash-filter-multiline
COPY config/logstash.yml /usr/share/logstash/config/logstash.yml
COPY pipeline /usr/share/logstash/pipeline
