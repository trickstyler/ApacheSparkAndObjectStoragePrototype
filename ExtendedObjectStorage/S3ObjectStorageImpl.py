import logging
from botocore.exceptions import ClientError
from interface import implements

import boto3
import ntpath

from CloudObjectStorageInterface import CloudObjectStorageInterface


class S3ObjectStorageImpl(implements(CloudObjectStorageInterface)):
    def __init__(self):
        self.s3 = boto3.client('s3')

    def create_object(self, bucket_name, key, path_to_file):
        try:
            return self.s3.upload_file(path_to_file, bucket_name, key)
        except ClientError as e:
            logging.error(e)
            raise e

    def get_object(self, bucket_name, key, path_to_file):
        self.s3.download_file(bucket_name, key, path_to_file)

    def rename_object(self, source_bucket, source_key, dest_bucket, dest_key):

        response = self.s3.copy_object(Bucket=dest_bucket,
                                       CopySource={'Bucket': source_bucket,
                                                   'Key': source_key},
                                       Key=dest_key)

        # s3_resource = boto3.resource('s3')
        # copy_source = {
        #     'Bucket': source_bucket,
        #     'Key': source_key
        # }
        # response = s3_resource.meta.client.copy(copy_source, dest_bucket, dest_key)

        if (response is not None) and (response['CopyObjectResult']['ETag'] is not None):
            self.delete_object(source_bucket, source_key)

    def delete_object(self, bucket_name, key):
        return self.s3.delete_object(Bucket=bucket_name, Key=key)

    def create_directory(self, bucket_name, key):
        if key.endswith('/'):
            self.s3.put_object(Bucket=bucket_name, Key=key)

    def list_directory(self, bucket_name, key):
        if key.endswith('/'):
            return self.s3.list_objects_v2(Bucket=bucket_name, Prefix=key)

    def rename_directory(self, bucket_name, source_dir_name, dest_dir_name):

        if not(source_dir_name.endwith('/') and dest_dir_name.endswith('/')):
            raise ValueError("All directory names should end with a forward slash '/'")

        obj_list = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=source_dir_name)

        if obj_list is not None:

            s3_resource = boto3.resource('s3')
            for curr_key in obj_list['Contents']:
                copy_source = {
                    'Bucket': obj_list['Name'],
                    'Key': curr_key['Key']
                }
                dest_key = dest_dir_name + ntpath.basename(curr_key['Key'])
                response = s3_resource.meta.client.copy(copy_source, obj_list['Name'], dest_key)

                if (response is not None) and (response['CopyObjectResult']['ETag'] is not None):
                    self.delete_object(bucket_name, curr_key['Key'])

            head_response = self.s3.head_object(Bucket=bucket_name, Key=source_dir_name)

            if (head_response is not None) and (head_response['ETag'] is not None):
                self.delete_object(bucket_name, source_dir_name)
