import base64
import os
import shutil
import MySQLdb
from googleapiclient import discovery
from googleapiclient import http
from oauth2client.client import GoogleCredentials

from urllib.parse import (
    urljoin,
    urlparse,
)

try:
    from pylib.gcloud import get_storage as get_gcloud_storage
    from pylib.mysql import mysql_from_url
except ImportError:
    def get_gcloud_storage():
        credentials = GoogleCredentials.get_application_default()
        return discovery.build('storage', 'v1', credentials=credentials)

    def mysql_from_url(url):
        parsed = urlparse(url)
        assert parsed.scheme == 'mysql', (
            'MySQL url must be of the form mysql://hostname:port/db'
        )
        return MySQLdb.connect(
            user=parsed.username or 'root',
            passwd=parsed.password or '',
            host=parsed.hostname,
            port=parsed.port or 3306,
            db=parsed.path.strip('/'),
        )


class LocalStore:
    def __init__(self, path):
        assert os.path.exists(path) and os.path.isdir(path), (
            'Provided path did not exist or was not a directory: %s' % path
        )
        self.path = path

    def save(self, buffer):
        path = os.path.join(self.path, buffer.path)

        folder = os.path.dirname(path)
        if not os.path.exists(folder):
            os.makedirs(folder)

        file_obj = buffer.get_rewound_file()
        with open(path, 'wb') as output_fo:
            shutil.copyfileobj(file_obj, output_fo)


class GCloudStore:
    def __init__(self, url):
        parsed = urlparse(url)
        assert parsed.scheme == 'gs', (
            'Google cloud url must be of the form gs://'
        )
        self.bucket = parsed.hostname
        self.objects = get_gcloud_storage().objects()
        self.prefix = (parsed.path or '').strip('/') + '/'

    def save(self, buffer):
        path = urljoin(self.prefix, buffer.path)

        file_obj = buffer.get_rewound_file()
        md5_base64 = base64.b64encode(buffer.md5).decode('ascii')
        self.objects.insert(
            media_body=http.MediaIoBaseUpload(
                file_obj, 'application/octet-stream'
            ),
            name=path,
            body={
                'md5Hash': md5_base64,
                'metadata': {
                    'count': str(buffer.count),
                },
            },
            bucket=self.bucket,
        ).execute()

class MySQLMetadataStore:
    def __init__(self, url):
        self.mysql = mysql_from_url(url)

    def save(self, buffer):
        cursor = None
        try:
            cursor = self.mysql.cursor()
            cursor.execute((
                'INSERT IGNORE INTO kafka_store (' +
                '`topic`, `partition`, `startOffset`, `finalOffset`, ' +
                '`byteSize`, `md5Hash`, `time`' +
                ') VALUES (%s, %s, %s, %s, %s, %s, NOW())'
            ), (
                buffer.topic, buffer.partition,
                buffer.first_offset, buffer.final_offset,
                buffer.byte_size, buffer.md5_hex,
            ))
            self.mysql.commit()
        finally:
            if cursor:
                cursor.close()
