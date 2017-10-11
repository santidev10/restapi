import csv
import logging
import os
import tempfile

import paramiko
from django.core.management import BaseCommand

from segment.models import SegmentChannel

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level='INFO')
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_DIR = os.path.join(BASE_DIR, 'commands')


class Command(BaseCommand):
    help = 'Utility task for segments recommended channels update'
    # machine learning reports host
    host = 'ec2-52-90-28-112.compute-1.amazonaws.com'
    username = 'ubuntu'
    auth_key = os.path.expanduser('~/.ssh/id_rsa')
    remote_dir = '/mnt/augmentation'
    paramiko.util.log_to_file('/tmp/paramiko.log')

    def handle(self, *args, **options):
        # setup connection to host machine
        k = paramiko.RSAKey.from_private_key_file(self.auth_key)
        c = paramiko.SSHClient()
        c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        c.connect(hostname=self.host, username=self.username, pkey=k)
        sftp = c.open_sftp()

        with tempfile.TemporaryDirectory() as tmpdirname:
            # collect ml reports from remote machine
            dir_items = sftp.listdir_attr(self.remote_dir)
            for item in dir_items:
                if item.filename.endswith('.csv'):
                    remote_path = os.path.join(self.remote_dir, item.filename)
                    local_path = os.path.join(tmpdirname, item.filename)
                    sftp.get(remote_path, local_path)
            sftp.close()
            c.close()
            file_list = os.listdir(tmpdirname)
            logger.info("Start update segments recommend channels procedure")
            for item in self.read_files(file_list, tmpdirname):
                self.update_segment(item)
            logger.info("Segments recommend channels update procedure finished")

    def update_segment(self, data):
        segment_id, channels_list = data
        try:
            segment = SegmentChannel.objects.get(id=segment_id)
            segment.top_recommend_channels = channels_list
            segment.save()
        except SegmentChannel.DoesNotExist:
            logger.info("No Segment with id {}".format(segment_id))

    def read_files(self, file_list, tmp_dir):
        for file in file_list:
            with open(os.path.join(tmp_dir, file), 'r') as csvfile:
                c = csv.reader(csvfile, delimiter=',')
                for item in c:
                    yield item[0], item[1].split(',')
