from django.core.management.base import BaseCommand
import logging
logger = logging.getLogger(__name__)
from pid import PidFile
import requests
import json
import sqlite3
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
import time

from es_components.connections import init_es_connection
init_es_connection()

class Command(BaseCommand):

    def handle(self, *args, **options):
        with PidFile(piddir='.', pidname='export_queue.pid') as p:
            pass