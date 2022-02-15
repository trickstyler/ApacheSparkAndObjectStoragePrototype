from interface import Interface


class ExtendedObjectStorageInterface(Interface):

    def create_object(self, bucket_name, key, path_to_file):
        pass

    def get_object(self, bucket_name, key, path_to_file):
        pass

    def rename_object(self, source_bucket, source_key, dest_bucket, dest_key):
        pass

    def delete_object(self, bucket_name, key):
        pass

    def create_directory(self, bucket_name, key):
        pass

    def list_directory(self, bucket_name, source_dir):
        pass

    def rename_directory(self, bucket_name, source_dir, dest_dir):
        pass
