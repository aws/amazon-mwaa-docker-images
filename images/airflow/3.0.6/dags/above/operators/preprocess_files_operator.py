import gc
import gzip
import json
import logging
import os
import re
import traceback
import warnings
import zipfile
from tempfile import TemporaryDirectory

import pandas as pd
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from fsplit.filesplit import Filesplit as FileSplit

# TODO add support for csv.gz
supported_file_extensions = [
    'csv',
    'json',
    'txt',
    'json.gz',
    'zip'
]


class PreprocessFilesBaseOperator(BaseOperator):

    def __init__(
            self,
            *args,
            **kwargs
    ):
        super(PreprocessFilesBaseOperator, self).__init__(*args, **kwargs)

    def download(self, s3_hook, key, local_dir):
        pass

    def upload(self, s3_hook, filename, key, bucket_name, **kwargs):
        logging.info('Started loading - {} to {}'.format(filename, key))
        s3_hook.load_file(
            filename=filename,
            key=key,
            bucket_name=bucket_name,
            replace=True,
            **kwargs
        )
        logging.info('Finished loading - {} to {}'.format(filename, key))

    def get_file_extension(self, filename):
        for file_ext in supported_file_extensions:
            if filename.endswith(file_ext):
                return file_ext

        warnings.warn('File extension not supported - {}'.format(filename))
        return None


class UnzipSplitFilesOperator(PreprocessFilesBaseOperator):
    """
    Takes files from /source, unzips and splits them into /source/tmp to
    prepare for preprocessing
    """

    def __init__(
            self,
            s3_conn_id,
            s3_in_bucket,
            s3_source_dir,
            s3_source_archive_dir,
            s3_in_temp_dir='tmp',
            *args,
            **kwargs
    ):
        super(UnzipSplitFilesOperator, self).__init__(*args, **kwargs)

        self.s3_conn_id = s3_conn_id
        self.s3_in_bucket = s3_in_bucket
        self.s3_source_dir = s3_source_dir
        self.s3_source_archive_dir = s3_source_archive_dir
        self.s3_in_temp_prefix = os.path.join(s3_source_dir, s3_in_temp_dir)

    def split_and_upload(
            self, filename, s3_hook, s3_bucket, s3_prefix='', splitsize=10 ** 8
    ):
        with TemporaryDirectory('w') as out_dir:
            # split the file into ~100MB chunks
            fs = FileSplit(
                file=filename, splitsize=splitsize, output_dir=out_dir
            )
            fs.split(include_header=True)

            # Load each of the splits into the temp s3 dir
            for out_file in os.listdir(out_dir):
                s3_out_path = os.path.join(s3_prefix, out_file)
                file_path = os.path.join(out_dir, out_file)

                super(UnzipSplitFilesOperator, self).upload(
                    s3_hook, filename=file_path, key=s3_out_path,
                    bucket_name=s3_bucket
                )

    def preprocess(self, s3_hook, file_path, temp_dir, s3_key):
        # TODO add support for csv.gz
        in_file = file_path.split('/')[-1]
        in_file_ext = super(UnzipSplitFilesOperator, self).get_file_extension(
            in_file
        )
        # will unzip (if necessary) and split files into chunks
        if in_file_ext == 'zip':
            subdir_prefix = '/' + in_file

            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

            file_list = [os.path.join(temp_dir, f)
                         for f in os.listdir(temp_dir)]

            # remove original zip file from list
            file_list.remove(os.path.join(temp_dir, in_file))

            # see if there are any files in this temp dir
            # if so, probably because the job died in the middle of a past run
            # clear them out and try again to be safe
            existing_temp_files = s3_hook.list_keys(
                bucket_name=self.s3_in_bucket,
                prefix=os.path.join(self.s3_in_temp_prefix, subdir_prefix)
            ) or []
            if len(existing_temp_files):
                logging.info(
                    'Deleting existing temp files - {}'.format(
                        ', '.join(existing_temp_files)
                    )
                )
                s3_hook.delete_objects(
                    bucket=self.s3_in_bucket, keys=existing_temp_files
                )

        elif in_file_ext is not None:
            # if it's not a zip file, we will only be processing a single file
            # in the loop
            source_file_name = s3_key.split('/')[-1]
            subdir_prefix = s3_key.replace(self.s3_source_dir, '').replace(
                source_file_name, ''
            )
            file_list = [file_path]

        else:
            logging.info('Skipping file - {}'.format(in_file))

            return False

        for f in file_list:
            logging.info('Processing - {}'.format(f))

            file_ext = super(UnzipSplitFilesOperator, self).get_file_extension(
                f
            )
            in_path = os.path.join(temp_dir, f)
            # out_path = os.path.join(out_dir, f)

            if file_ext == 'json.gz':
                # pass through gzipped files for now
                # later we can decide if we want to decompress -> split ->
                # upload to temp -> recompress
                # TODO add support for csv.gz - this will currently just pass
                #  it through
                out_file = in_file
                s3_out_path = self.s3_in_temp_prefix + subdir_prefix + out_file

                super(UnzipSplitFilesOperator, self).upload(
                    s3_hook, filename=in_path, key=s3_out_path,
                    bucket_name=self.s3_in_bucket
                )

            elif file_ext in ('csv', 'txt'):
                self.split_and_upload(
                    filename=f, s3_hook=s3_hook,
                    s3_bucket=self.s3_in_bucket,
                    s3_prefix=(self.s3_in_temp_prefix + subdir_prefix)
                )

            elif file_ext == 'json':
                line_count = sum(1 for line in open(f))
                if line_count == 1:
                    # read in the whole file convert to newline delimited json
                    # if memory becomes an issue here we can find a more
                    # efficient way
                    with open(f, 'r') as f_in:
                        pd.DataFrame(json.load(f_in)).to_json(
                            f, lines=True, orient='records'
                        )

                self.split_and_upload(
                    filename=f, s3_hook=s3_hook,
                    s3_bucket=self.s3_in_bucket,
                    s3_prefix=(self.s3_in_temp_prefix + subdir_prefix)
                )

        return True

    def execute(self, context):
        # TODO refactor to run in parallel (maybe - might make logs hard to
        #  follow)
        s3_hook = S3Hook(self.s3_conn_id)

        s3_file_list = s3_hook.list_keys(
            bucket_name=self.s3_in_bucket, prefix=self.s3_source_dir
        ) or []
        s3_temp_file_list = s3_hook.list_keys(
            bucket_name=self.s3_in_bucket, prefix=self.s3_in_temp_prefix
        ) or []

        # get a list of non-temp files
        # these files need to be run through both steps
        s3_files_less_temp_files = set(s3_file_list) - set(s3_temp_file_list)
        for key in s3_files_less_temp_files:
            source_file_name = key.split('/')[-1]

            if source_file_name.strip() == '':
                logging.info('Skipping file - {}'.format(key))
                continue

            # do everything within context of temp dir
            with TemporaryDirectory('w') as temp_dir:
                in_path = os.path.join(temp_dir, source_file_name)

                # get file from s3
                logging.info(
                    'Started getting {} from S3 bucket {}'.format(
                        key, self.s3_in_bucket
                    )
                )
                obj = s3_hook.get_key(key, self.s3_in_bucket)
                obj.download_file(in_path)
                logging.info(
                    'Finished getting {} from S3 bucket {}'.format(
                        key, self.s3_in_bucket
                    )
                )

                success = self.preprocess(s3_hook, in_path, temp_dir, key)

                if success:
                    # once pre_preprocessing is done, upload the original
                    # file to the source_archive_dir
                    s3_hook.load_file(
                        filename=in_path,
                        key=key.replace(
                            self.s3_source_dir,
                            self.s3_source_archive_dir
                        ),
                        bucket_name=self.s3_in_bucket,
                        replace=True
                    )

                    # remove file from source dir
                    s3_hook.delete_objects(bucket=self.s3_in_bucket, keys=[key])


class PreprocessFilesOperator(PreprocessFilesBaseOperator):

    def __init__(
            self,
            s3_conn_id,
            s3_in_bucket,
            s3_out_bucket,
            s3_source_dir,
            s3_error_dir,
            s3_success_dir,
            s3_preprocessed_dir,
            s3_in_temp_dir='tmp',
            file_parsing_args={},
            custom_file_converter=None,
            *args,
            **kwargs
    ):
        super(PreprocessFilesOperator, self).__init__(*args, **kwargs)

        self.s3_conn_id = s3_conn_id
        self.s3_in_bucket = s3_in_bucket
        self.s3_in_temp_prefix = os.path.join(s3_source_dir, s3_in_temp_dir)
        self.s3_out_bucket = s3_out_bucket

        self.s3_source_dir = s3_source_dir
        self.s3_preprocessed_dir = s3_preprocessed_dir
        self.s3_error_dir = s3_error_dir
        self.s3_success_dir = s3_success_dir

        self.file_parsing_args = file_parsing_args
        self.custom_file_converter = custom_file_converter

    def preprocess(self, s3_hook, s3_client, key):
        with TemporaryDirectory('w') as temp_dir:
            logging.info('Started preprocessing - {}'.format(key))
            source_file_name = key.split('/')[-1]

            if source_file_name.strip() == '' or \
                    s3_client.head_object(Bucket=self.s3_in_bucket, Key=key)[
                        'ContentLength'] <= 1:
                logging.info('Skipping file - {}'.format(key))
                return {
                    'skipped': f"https://s3.console.aws.amazon.com/s3/object/"
                               f"{self.s3_in_bucket}/{key}"
                }

            file_ext = super(PreprocessFilesOperator, self).get_file_extension(
                source_file_name
            )

            in_path = os.path.join(temp_dir, source_file_name)

            logging.info(
                'Started getting {} from S3 bucket {}'.format(
                    key, self.s3_in_bucket
                )
            )
            obj = s3_hook.get_key(key, self.s3_in_bucket)
            obj.download_file(in_path)
            logging.info(
                'Finished getting {} from S3 bucket {}'.format(
                    key, self.s3_in_bucket
                )
            )

            out_dir = os.path.join(temp_dir, 'json')
            os.makedirs(out_dir)

            # TODO add support for csv.gz
            if file_ext == 'json.gz':
                s3_out_path = key.replace(
                    self.s3_in_temp_prefix, self.s3_preprocessed_dir
                )
                super(PreprocessFilesOperator, self).upload(
                    s3_hook, filename=in_path, key=s3_out_path,
                    bucket_name=self.s3_in_bucket
                )

                # put the original file to success dir
                s3_success_out_path = key.replace(
                    self.s3_in_temp_prefix, self.s3_success_dir
                )
                super(PreprocessFilesOperator, self).upload(
                    s3_hook, filename=in_path, key=s3_success_out_path,
                    bucket_name=self.s3_out_bucket
                )

                result = {
                    'success': f'https://s3.console.aws.amazon.com/s3/object/'
                               f'{self.s3_out_bucket}/{s3_success_out_path}'
                }

            elif file_ext in ('csv', 'txt', 'json'):
                json_file_name = source_file_name.replace(
                    '.{}'.format(file_ext), '.json.gz'
                )
                logging.info('json file name --- {}'.format(json_file_name))
                out_path = os.path.join(out_dir, json_file_name)

                logging.info(
                    'Started preprocessing {} - converting to '
                    'json.gz'.format(
                        source_file_name
                    )
                )
                try:
                    # if custom_file_converter is passed, use it
                    # custom file converter MUST accept 2 args - in_path &
                    # out_path
                    # this function should take the delimited file in and
                    # output it as json.gz to out_path
                    if self.custom_file_converter:
                        self.custom_file_converter(in_path, out_path)
                    else:
                        # read df in chunks and convert to json.gz
                        regexStr = r'[^a-zA-Z0-9\_\$]'
                        with gzip.open(out_path, 'wt') as f_gz:
                            for c in pd.read_csv(
                                    in_path, chunksize=1000,
                                    **self.file_parsing_args
                            ):
                                c.rename(
                                    columns=lambda x: re.sub(
                                        regexStr, "_", x
                                    ),
                                    inplace=True
                                )  # rename columns to make them SQL
                                # compatible
                                c = c.loc[:,
                                    ~c.columns.duplicated()]  # remove
                                # duplicated columns
                                c.to_json(
                                    f_gz, lines=True, orient='records',
                                    compression='gzip'
                                )

                    logging.info(
                        'Finished preprocessing {} - converted to '
                        'json.gz'.format(
                            source_file_name
                        )
                    )

                    # put the converted file to preprocessed dir
                    s3_preprocessed_out_path = key.replace(
                        self.s3_in_temp_prefix, self.s3_preprocessed_dir
                    ).replace(source_file_name, json_file_name)
                    super(PreprocessFilesOperator, self).upload(
                        s3_hook, filename=out_path,
                        key=s3_preprocessed_out_path,
                        bucket_name=self.s3_out_bucket
                    )

                    # put the original file to success dir
                    s3_success_out_path = key.replace(
                        self.s3_in_temp_prefix, self.s3_success_dir
                    )
                    super(PreprocessFilesOperator, self).upload(
                        s3_hook, filename=in_path, key=s3_success_out_path,
                        bucket_name=self.s3_out_bucket
                    )

                    result = {
                        'success': f'https://s3.console.aws.amazon.com/s3'
                                   f'/object'
                                   f'/{self.s3_out_bucket}'
                                   f'/{s3_success_out_path}'
                    }

                except Exception as e:
                    logging.exception(
                        'Error preprocessing - {}'.format(source_file_name)
                    )
                    s3_out_path = key.replace(
                        self.s3_in_temp_prefix, self.s3_error_dir
                    )

                    super(PreprocessFilesOperator, self).upload(
                        s3_hook, filename=in_path, key=s3_out_path,
                        bucket_name=self.s3_out_bucket
                    )

                    # upload log stack trace so we can see it in the bucket
                    # next to the file
                    exception_file_path = os.path.join(
                        temp_dir, source_file_name.replace(
                            file_ext, 'log'
                        )
                    )
                    s3_exception_out_path = s3_out_path.replace(file_ext, 'log')
                    with open(exception_file_path, 'w') as f:
                        f.write(str(e))
                        f.write(traceback.format_exc())

                    super(PreprocessFilesOperator, self).upload(
                        s3_hook, filename=exception_file_path,
                        key=s3_exception_out_path,
                        bucket_name=self.s3_out_bucket
                    )

                    result = {
                        'error': f'https://s3.console.aws.amazon.com/s3/object/'
                                 f'{self.s3_out_bucket}/{s3_out_path}'
                    }

        return result

    def execute(self, context):
        # TODO refactor to run in parallel (maybe - might make logs hard to
        #  follow)
        s3_hook = S3Hook(self.s3_conn_id)
        s3_client = s3_hook.get_conn()

        s3_temp_file_list = s3_hook.list_keys(
            bucket_name=self.s3_in_bucket, prefix=self.s3_in_temp_prefix
        ) or []
        s3_temp_file_list = iter(s3_temp_file_list)

        # iterate over all files and apply preprocessing
        results = []
        for key in s3_temp_file_list:
            meta = s3_client.head_object(Bucket=self.s3_in_bucket, Key=key)
            logging.info(key)
            logging.info(meta['ContentLength'])

            result = self.preprocess(s3_hook, s3_client, key)
            results.append(result)
            s3_hook.delete_objects(bucket=self.s3_in_bucket, keys=[key])

            # force memory cleanup
            gc.collect()

        collapsed_results = {}
        for d in results:
            for k, v in d.items():
                if k in collapsed_results:
                    collapsed_results[k].append(v)
                else:
                    collapsed_results[k] = [v]
        return collapsed_results
