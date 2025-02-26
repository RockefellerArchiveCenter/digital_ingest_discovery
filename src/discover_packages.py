import json
import logging
import os
import tarfile
import traceback
from pathlib import Path
from shutil import copy, copyfileobj

import rac_schema_validator
import requests

from .helpers import get_client_with_role, validate_package_data

logging.basicConfig(
    level=int(os.environ.get('LOGGING_LEVEL', logging.INFO)),
    format='%(filename)s::%(funcName)s::%(lineno)s %(message)s')


class PackageDiscoverer(object):

    def __init__(self,
                 package_id,
                 digitization_path,
                 digitization_url,
                 role_arn,
                 sns_topic,
                 source_bucket,
                 storage_dir,
                 tmp_dir):
        self.package_id = package_id
        self.digitization_path = digitization_path
        self.digitization_url = digitization_url
        self.role_arn = role_arn
        self.service_name = 'ursa_major'
        self.sns_topic = sns_topic
        self.source_bucket = source_bucket
        self.storage_dir = storage_dir
        self.tmp_dir = tmp_dir
        for fp in [self.digitization_path, self.storage_dir, self.tmp_dir]:
            Path(fp).mkdir(exist_ok=True)

    def run(self):
        logging.debug(
            f'Discovery started for package {self.package_id}.')
        try:
            self.deliver_start_notification()
            downloaded_path = Path(self.tmp_dir, f"{self.package_id}.tar.gz")
            self.download(downloaded_path)
            package_path, package_data = self.unpack(downloaded_path)
            if package_data.get('origin') == 'digitization':
                self.deliver_to_digitization(package_path, package_data)
            self.cleanup_successful_job(downloaded_path)
            self.deliver_success_notification(package_path, package_data)
            logging.info(
                f'Package {self.package_id} successfully discovered.')
        except Exception as e:
            logging.exception(e)
            self.cleanup_failed_job(downloaded_path)
            self.deliver_failure_notification(e)

    def download(self, download_path):
        """Downloads package from S3 bucket.

        Returns:
            package_path (pathlib.Path): path to package binary
        """
        s3_client = get_client_with_role('s3', self.role_arn)
        s3_client.download_file(
            self.source_bucket,
            f"{self.package_id}.tar.gz",
            download_path)
        logging.debug(f'Package downloaded to {download_path}.')
        return download_path

    def unpack(self, downloaded_path):
        """Unzips package and handles contents.

        Args:
            downloaded_path (pathlib.Path): location of downloaded package binary.

        Returns:
            package_path (pathlib.Path): location of unpacked package.
            data (dict): data about the package
        """
        with tarfile.open(downloaded_path, "r:gz") as outer_tar:
            """Extract and validate JSON data"""
            json_file = outer_tar.extractfile(f"{self.package_id}/{self.package_id}.json")
            package_data = json.load(json_file)
            try:
                validate_package_data(package_data, f"{package_data.get('origin', 'aurora')}_bag.json")
            except rac_schema_validator.exceptions.ValidationError as e:
                raise Exception(
                    f"Invalid package data: {e} \n{package_data}")

            """Move Aurora package URL (if it exists) to identifiers"""
            try:
                aurora_url = package_data.pop('url')
                package_data.setdefault('identifiers', {}).update({'aurora_package': aurora_url})
            except KeyError:
                pass

            """Move metadata title to title key"""
            if not package_data.get('title'):
                package_data['title'] = package_data['metadata']['title']

            """Extract and save nested package binary as .tar.gz"""
            inner_tar_data = outer_tar.extractfile(f"{self.package_id}/{self.package_id}.tar.gz")
            storage_path = Path(self.storage_dir, f"{self.package_id}.tar.gz")
            with open(storage_path, "wb") as package_binary:
                copyfileobj(inner_tar_data, package_binary)

        logging.debug(f'Package unpacked to {self.storage_dir}.')
        return storage_path, package_data

    def deliver_to_digitization(self, package_path, package_data):
        """Deliver package to digitization services.

        Args:
            package_path (pathlib.Path): location of the package binary
            data (dict): data about the package
        """
        """Copy package binary"""
        derivative_path = Path(self.digitization_path, self.package_id)
        copy(package_path, derivative_path)

        """Create package object in digitization service"""
        try:
            r = requests.post(
                self.digitization_url,
                json={
                    "bag_data": package_data,
                    "origin": package_data['origin'],
                    "identifier": package_data['identifier']},
                headers={"Content-Type": "application/json"},
            )
            r.raise_for_status()
            logging.debug(
                f'Package delivereed to digitization services at location {derivative_path} and URL {self.digitization_url}.')
        except requests.exceptions.HTTPError as e:
            if r.text:
                raise Exception(r.text)
            else:
                raise e

    def cleanup_successful_job(self, downloaded_path):
        """Remove temporary files created during processing.

        Args:
            downloaded_path (pathlib.Path): location of downloaded package binary
        """
        downloaded_path.unlink(missing_ok=True)
        logging.debug('Cleanup from successful job completed.')

    def cleanup_failed_job(self, downloaded_path):
        """Remove all files created during processing.

        Args:
            downloaded_path (pathlib.Path): location of downloaded package binary
            package_path (pathlib.Path): location of the package binary
        """
        downloaded_path.unlink(missing_ok=True)
        logging.debug('Cleanup from failed job completed.')

    def deliver_start_notification(self):
        client = get_client_with_role('sns', self.role_arn)
        client.publish(
            TopicArn=self.sns_topic,
            Message=f'Discovery for {self.package_id} started.',
            MessageAttributes={
                'package_id': {
                    'DataType': 'String',
                    'StringValue': self.package_id,
                },
                'service': {
                    'DataType': 'String',
                    'StringValue': self.service_name,
                },
                'outcome': {
                    'DataType': 'String',
                    'StringValue': 'STARTED',
                }
            })
        logging.debug('Start notification delivered.')

    def deliver_success_notification(self, package_path, package_data):
        """Send SNS message about successful job.

        Args:
            package_path (pathlib.Path): location of the package binary
            data (dict): data about the package
        """
        client = get_client_with_role('sns', self.role_arn)
        package_data['package_path'] = package_path
        client.publish(
            TopicArn=self.sns_topic,
            Message=f'Package {self.package_id} successfully discovered.',
            MessageAttributes={
                'package_id': {
                    'DataType': 'String',
                    'StringValue': self.package_id,
                },
                'service': {
                    'DataType': 'String',
                    'StringValue': self.service_name,
                },
                'outcome': {
                    'DataType': 'String',
                    'StringValue': 'SUCCESS',
                },
                'package_data': {
                    'DataType': 'String',
                    'StringValue': json.dumps(package_data),
                },
            })
        logging.debug('Success notification delivered.')

    def deliver_failure_notification(self, exception):
        """Send SNS message about failed job.

        Args:
            package_path (pathlib.Path): location of the package binary
            data (dict): data about the package
            exception (Exception): the exception that was thrown.
        """
        client = get_client_with_role('sns', self.role_arn)
        tb = ''.join(traceback.format_exception(exception)[:-1])
        client.publish(
            TopicArn=self.sns_topic,
            Message=f'Package {self.package_id} failed during discovery.',
            MessageAttributes={
                'package_id': {
                    'DataType': 'String',
                    'StringValue': self.package_id,
                },
                'service': {
                    'DataType': 'String',
                    'StringValue': self.service_name,
                },
                'outcome': {
                    'DataType': 'String',
                    'StringValue': 'FAILURE',
                },
                'message': {
                    'DataType': 'String',
                    'StringValue': str(exception),
                },
                'traceback': {
                    'DataType': 'String',
                    'StringValue': tb,
                }
            })
        logging.debug('Failure notification delivered.')


if __name__ == '__main__':
    package_id = os.environ.get('PACKAGE_ID')
    digitization_path = os.environ.get('DIGITIZATION_PATH')
    digitization_url = os.environ.get('DIGITIZATION_URL')
    role_arn = os.environ.get('AWS_ROLE_ARN')
    sns_topic = os.environ.get('AWS_SNS_TOPIC')
    source_bucket = os.environ.get('AWS_SOURCE_BUCKET')
    storage_dir = os.environ.get('STORAGE_DIR')
    tmp_dir = os.environ.get('TMP_DIR')

    PackageDiscoverer(
        package_id,
        digitization_path,
        digitization_path,
        role_arn,
        sns_topic,
        source_bucket,
        storage_dir,
        tmp_dir
    ).run()
