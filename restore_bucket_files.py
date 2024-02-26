import boto3
import argparse
import sys
from datetime import datetime, timezone

parser = argparse.ArgumentParser(prog='restore_bucket_files.py',
                                 description='restore files in bucket to date',
                                 epilog='Usage:')
parser.add_argument('bucket_name')
parser.add_argument('restore_date', help='for example 2024-2-21T11:00:00')

args = parser.parse_args()

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

bucket_name = args.bucket_name
try:
    bucket_date = datetime.strptime(args.restore_date, "%Y-%m-%dT%H:%M:%S")
    bucket_date = bucket_date.replace(tzinfo=timezone.utc)
except ValueError:
    print("Please use YYYY-MM-DDTHH:MM:SS format, example: 2024-2-21T11:00:00")
    sys.exit(1)

my_bucket = s3.Bucket(bucket_name)

for s3_object in my_bucket.objects.all():
    versions = s3_client.list_object_versions(
        Bucket=bucket_name,
        Prefix=s3_object.key
    )
    closest_ver = None
    closest_ver_diff = None
    versions_count = len(versions.get('Versions', [])) \
        + len(versions.get('DeleteMarkers', []))

    if versions_count > 1:
        for version in versions.get('Versions', []):
            if version['Key'] == s3_object.key:
                if not version['IsLatest']:
                    last_modified = version['LastModified']
                    if last_modified == bucket_date:
                        version_id = version['VersionId']
                    else:
                        time_diff = \
                            abs((last_modified - bucket_date).total_seconds())
                        if closest_ver_diff is None \
                                or time_diff < closest_ver_diff:
                            closest_ver_diff = time_diff
                            closest_ver = version['VersionId']

        if closest_ver:
            print(f"Restored file {s3_object.key} to: {closest_ver}")
            copy_source = {
              'Bucket': bucket_name,
              'Key': s3_object.key,
              'VersionId': closest_ver
            }
            s3.meta.client.copy(copy_source, bucket_name, s3_object.key)
