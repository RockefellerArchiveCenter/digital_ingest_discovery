import json
from pathlib import Path
from shutil import copy
from unittest.mock import patch

import boto3
import pytest
from moto import mock_s3, mock_sns, mock_sqs, mock_sts
from moto.core import DEFAULT_ACCOUNT_ID

from src.discover_packages import PackageDiscoverer

ARGS = [
    'f78742e5-6af9-4756-a94a-6cd297406d50',  # package_id
    '/digitization',  # digitization_path
    'https://zorya.rockarch.org/package',  # digitization_url
    'ursa-major-dev-role-arn',  # role_arn
    'topic',  # sns_topic
    'digital-ingest-upload',  # source_bucket
    '/storage',  # storage_dir
    '/dev/ursa_major',  # ssm_parameter_path
    '/tmp',  # tmp_dir
]


def test_init():
    """Asserts Validator init method sets attributes correctly."""
    discoverer = PackageDiscoverer(*ARGS)
    assert discoverer.package_id == 'f78742e5-6af9-4756-a94a-6cd297406d50'
    assert discoverer.digitization_path == '/digitization'
    assert discoverer.digitization_url == 'https://zorya.rockarch.org/package'
    assert discoverer.role_arn == 'ursa-major-dev-role-arn'
    assert discoverer.sns_topic == 'topic'
    assert discoverer.source_bucket == 'digital-ingest-upload'
    assert discoverer.storage_dir == '/storage'
    assert discoverer.ssm_parameter_path == '/dev/ursa_major'
    assert discoverer.tmp_dir == '/tmp'

    invalid_args = [
        'f78742e5-6af9-4756-a94a-6cd297406d50',
        '/digit tmp_dirization',
        'https://zorya.rockarch.org/package',
        'ursa-major-dev-role-arn',
        'topic',
        'digital-ingest-upload',
        '/storage',
        '/dev/ursa_major']
    with pytest.raises(Exception):
        PackageDiscoverer(*invalid_args)


@patch('src.discover_packages.PackageDiscoverer.download')
@patch('src.discover_packages.PackageDiscoverer.unpack')
@patch('src.discover_packages.PackageDiscoverer.deliver_to_digitization')
@patch('src.discover_packages.PackageDiscoverer.cleanup_successful_job')
@patch('src.discover_packages.PackageDiscoverer.deliver_success_notification')
@patch('src.discover_packages.PackageDiscoverer.cleanup_failed_job')
@patch('src.discover_packages.PackageDiscoverer.deliver_failure_notification')
def test_run_born_digital(
        mock_failure_notification,
        mock_failed_cleanup,
        mock_success_notification,
        mock_success_cleanup,
        mock_deliver,
        mock_unpack,
        mock_download):
    """Ensures born digital packages trigger the correct methods and arguments."""
    discoverer = PackageDiscoverer(*ARGS)
    download_path = Path(discoverer.tmp_dir, f"{discoverer.package_id}.tar.gz")
    storage_path = Path(discoverer.storage_dir, discoverer.package_id)

    package_data = {"origin": "aurora"}
    mock_unpack.return_value = storage_path, package_data
    discoverer.run()
    mock_failure_notification.assert_not_called()
    mock_failed_cleanup.assert_not_called()
    mock_success_notification.assert_called_once_with(storage_path, package_data)
    mock_success_cleanup.assert_called_once_with(download_path)
    mock_deliver.assert_not_called()
    mock_unpack.assert_called_once_with(download_path)
    mock_download.assert_called_once_with(download_path)


@patch('src.discover_packages.PackageDiscoverer.download')
@patch('src.discover_packages.PackageDiscoverer.unpack')
@patch('src.discover_packages.PackageDiscoverer.deliver_to_digitization')
@patch('src.discover_packages.PackageDiscoverer.cleanup_successful_job')
@patch('src.discover_packages.PackageDiscoverer.deliver_success_notification')
@patch('src.discover_packages.PackageDiscoverer.cleanup_failed_job')
@patch('src.discover_packages.PackageDiscoverer.deliver_failure_notification')
def test_run_digitized(
        mock_failure_notification,
        mock_failed_cleanup,
        mock_success_notification,
        mock_success_cleanup,
        mock_deliver,
        mock_unpack,
        mock_download):
    """Ensures digitized packages trigger the correct methods and arguments."""
    discoverer = PackageDiscoverer(*ARGS)
    download_path = Path(discoverer.tmp_dir, f"{discoverer.package_id}.tar.gz")
    storage_path = Path(discoverer.storage_dir, discoverer.package_id)

    # Happy path, digitized content
    package_data = {"origin": "digitization"}
    mock_unpack.return_value = storage_path, package_data
    discoverer.run()
    mock_failure_notification.assert_not_called()
    mock_failed_cleanup.assert_not_called()
    mock_success_notification.assert_called_once_with(storage_path, package_data)
    mock_success_cleanup.assert_called_once_with(download_path)
    mock_deliver.assert_called_once_with(storage_path, package_data)
    mock_unpack.assert_called_once_with(download_path)
    mock_download.assert_called_once_with(download_path)


@patch('src.discover_packages.PackageDiscoverer.download')
@patch('src.discover_packages.PackageDiscoverer.cleanup_failed_job')
@patch('src.discover_packages.PackageDiscoverer.deliver_failure_notification')
def test_run_exception(
        mock_failure_notification,
        mock_failed_cleanup,
        mock_download):
    """Ensures exceptions are handled correctly."""
    discoverer = PackageDiscoverer(*ARGS)
    exception = Exception("Invalid refid.")
    mock_download.side_effect = exception
    download_path = Path(discoverer.tmp_dir, f"{discoverer.package_id}.tar.gz")
    discoverer.run()
    mock_failure_notification.assert_called_once_with(exception)
    mock_failed_cleanup.assert_called_once_with(download_path)


@mock_s3
@mock_sts
@patch('src.discover_packages.get_client_with_role')
def test_download(mock_role):
    discoverer = PackageDiscoverer(*ARGS)
    s3 = boto3.client('s3', region_name='us-east-1')
    mock_role.return_value = s3
    s3.create_bucket(Bucket=discoverer.source_bucket)
    s3.put_object(
        Bucket=discoverer.source_bucket,
        Key=f"{discoverer.package_id}.tar.gz",
        Body='')
    download_path = Path(discoverer.tmp_dir, f"{discoverer.package_id}.tar.gz")

    discoverer.download(download_path)
    download_path.is_file()


def test_unpack():
    discoverer = PackageDiscoverer(*ARGS)
    fixture_path = Path(
        "tests",
        "fixtures",
        "bags",
        "f78742e5-6af9-4756-a94a-6cd297406d50.tar.gz")
    tmp_path = Path(discoverer.tmp_dir, f"{discoverer.package_id}.tar.gz")
    copy(fixture_path, tmp_path)
    Path(discoverer.storage_dir).mkdir(exist_ok=True)

    package_path, package_data = discoverer.unpack(tmp_path)

    assert package_path == Path(discoverer.storage_dir, f"{discoverer.package_id}.tar.gz")
    assert package_path.is_file()
    with open(Path("tests", "fixtures", "json", "f78742e5-6af9-4756-a94a-6cd297406d50.json"), "r") as df:
        expected_data = json.load(df)
        assert package_data == expected_data


@patch('requests.post')
def test_deliver_to_digitization(mock_post):
    """Tests binary is correctly moved and POST request is sent with correct data."""
    discoverer = PackageDiscoverer(*ARGS)
    fixture_path = Path(
        "tests",
        "fixtures",
        "bags",
        "f78742e5-6af9-4756-a94a-6cd297406d50.tar.gz")
    storage_path = Path(discoverer.storage_dir, f"{discoverer.package_id}.tar.gz")
    for fp in [discoverer.storage_dir, discoverer.digitization_path]:
        Path(fp).mkdir(exist_ok=True)
    copy(fixture_path, storage_path)
    package_data = {"origin": "digitization", "identifier": "f78742e5-6af9-4756-a94a-6cd297406d50"}

    discoverer.deliver_to_digitization(storage_path, package_data)

    Path(discoverer.digitization_path, f"{discoverer.package_id}.tar.gz").is_file()
    mock_post.assert_called_once_with(
        discoverer.digitization_url,
        json={
            "bag_data": package_data,
            "origin": "digitization",
            "identifier": "f78742e5-6af9-4756-a94a-6cd297406d50"},
        headers={'Content-Type': 'application/json'})


def test_cleanup_successful_job():
    """Ensures file are cleaned up as expected."""
    discoverer = PackageDiscoverer(*ARGS)
    fixture_path = Path(
        "tests",
        "fixtures",
        "bags",
        "f78742e5-6af9-4756-a94a-6cd297406d50.tar.gz")
    tmp_path = Path(discoverer.tmp_dir, discoverer.package_id)
    copy(fixture_path, tmp_path)
    discoverer.cleanup_successful_job(tmp_path)
    assert not tmp_path.is_dir()


def test_cleanup_failed_job():
    """Ensures file are cleaned up as expected."""
    discoverer = PackageDiscoverer(*ARGS)
    fixture_path = Path(
        "tests",
        "fixtures",
        "bags",
        "f78742e5-6af9-4756-a94a-6cd297406d50.tar.gz")
    tmp_path = Path(discoverer.tmp_dir, discoverer.package_id)
    copy(fixture_path, tmp_path)
    discoverer.cleanup_failed_job(tmp_path)
    assert not tmp_path.is_dir()


@mock_sns
@mock_sqs
@mock_sts
@patch('src.discover_packages.get_client_with_role')
def test_deliver_success_notification(mock_role):
    """Asserts success messages are delivered as expected."""
    sns = boto3.client('sns', region_name='us-east-1')
    mock_role.return_value = sns
    topic_arn = sns.create_topic(Name='my-topic')['TopicArn']
    sqs_conn = boto3.resource("sqs", region_name="us-east-1")
    sqs_conn.create_queue(QueueName="test-queue")
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=f"arn:aws:sqs:us-east-1:{DEFAULT_ACCOUNT_ID}:test-queue",
    )

    default_args = ARGS
    default_args[4] = topic_arn
    discoverer = PackageDiscoverer(*default_args)

    package_path = f"/tmp/{default_args[0]}"
    package_data = {}
    discoverer.deliver_success_notification(package_path, package_data)

    queue = sqs_conn.get_queue_by_name(QueueName="test-queue")
    messages = queue.receive_messages(MaxNumberOfMessages=1)
    message_body = json.loads(messages[0].body)
    assert message_body['MessageAttributes']['outcome']['Value'] == 'SUCCESS'
    assert message_body['MessageAttributes']['package_id']['Value'] == discoverer.package_id
    assert message_body['MessageAttributes']['service']['Value'] == discoverer.service_name

    json_data = json.loads(message_body['MessageAttributes']['package_data']['Value'])
    assert isinstance(json_data, dict)
    assert json_data['package_path'] == package_path


@mock_sns
@mock_sqs
@mock_sts
@patch('src.discover_packages.get_client_with_role')
@patch('traceback.format_exception')
def test_deliver_failure_notification(mock_traceback, mock_role):
    """Asserts failure messages are delivered as expected."""
    sns = boto3.client('sns', region_name='us-east-1')
    mock_role.return_value = sns
    topic_arn = sns.create_topic(Name='my-topic')['TopicArn']
    sqs_conn = boto3.resource("sqs", region_name="us-east-1")
    sqs_conn.create_queue(QueueName="test-queue")
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=f"arn:aws:sqs:us-east-1:{DEFAULT_ACCOUNT_ID}:test-queue",
    )

    default_args = ARGS
    default_args[4] = topic_arn
    discoverer = PackageDiscoverer(*default_args)
    exception_message = "foo"
    exception = Exception(exception_message)
    mock_traceback.return_value = ["baz", "buzz"]

    discoverer.deliver_failure_notification(exception)

    queue = sqs_conn.get_queue_by_name(QueueName="test-queue")
    messages = queue.receive_messages(MaxNumberOfMessages=1)
    message_body = json.loads(messages[0].body)
    assert message_body['MessageAttributes']['outcome']['Value'] == 'FAILURE'
    assert message_body['MessageAttributes']['package_id']['Value'] == discoverer.package_id
    assert exception_message in message_body['MessageAttributes']['message']['Value']
    assert message_body['MessageAttributes']['traceback']['Value'] == 'baz'
