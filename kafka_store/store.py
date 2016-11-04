import base64
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

PATH = '%(topic)s/%(partition)06d/%(offset)020d'

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
        path = urljoin(self.prefix, PATH % {
            'topic': buffer.topic,
            'partition': buffer.partition,
            'offset': buffer.first_offset,
        })

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

class MySQLStore:
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
