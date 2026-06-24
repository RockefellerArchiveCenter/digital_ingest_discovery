import io
import json
from pathlib import Path
from shutil import copy
from unittest.mock import call, patch

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID

from src.discover_packages import PackageDiscoverer

ARGS = [
    'f78742e5-6af9-4756-a94a-6cd297406d50',  # package_id
    'https://as.rockarch.org/api',  # AS baseurl
    'admin',  # AS username
    'admin',  # AS password
    'rac-dev-iiif-upload',  # iiif-bucket
    'digital-ingest-discovery-dev-s3-role-arn',  # s3_role_arn
    'digital-ingest-discovery-dev-sns-role-arn',  # sns_role_arn
    'topic',  # sns_topic
    'digital-ingest-upload',  # source_bucket
    'rac-dev-digital-ingest-assembly',  # assembly bucket
    '/tmp',  # ebs mount path
]


@patch('asnake.client.ASnakeClient.authorize')
def test_init(mock_as):
    """Asserts Validator init method sets attributes correctly."""
    mock_as.return_value = True
    discoverer = PackageDiscoverer(*ARGS)
    assert discoverer.package_id == 'f78742e5-6af9-4756-a94a-6cd297406d50'
    assert discoverer.iiif_bucket == 'rac-dev-iiif-upload'
    assert discoverer.s3_role_arn == 'digital-ingest-discovery-dev-s3-role-arn'
    assert discoverer.sns_role_arn == 'digital-ingest-discovery-dev-sns-role-arn'
    assert discoverer.sns_topic == 'topic'
    assert discoverer.source_bucket == 'digital-ingest-upload'
    assert discoverer.assembly_bucket == 'rac-dev-digital-ingest-assembly'
    assert discoverer.ebs_path == '/tmp'

    invalid_args = [
        'f78742e5-6af9-4756-a94a-6cd297406d50',
        'https://as.rockarch.org/api',
        'admin',
        'admin'
        'digital-ingest-discovery-dev-role-arn',
        'topic',
        'digital-ingest-upload',
        '/storage',
        '/dev/digital_ingest_discovery']
    with pytest.raises(Exception):
        PackageDiscoverer(*invalid_args)


@patch('src.discover_packages.PackageDiscoverer.download')
@patch('src.discover_packages.PackageDiscoverer.unpack')
@patch('src.discover_packages.PackageDiscoverer.deliver_to_iiif_pipeline')
@patch('src.discover_packages.PackageDiscoverer.get_package_size')
@patch('src.discover_packages.PackageDiscoverer.cleanup_successful_job')
@patch('src.discover_packages.PackageDiscoverer.deliver_success_notification')
@patch('src.discover_packages.PackageDiscoverer.deliver_failure_notification')
@patch('asnake.client.ASnakeClient.authorize')
def test_run_born_digital(
        mock_as_auth,
        mock_failure_notification,
        mock_success_notification,
        mock_success_cleanup,
        mock_package_size,
        mock_deliver,
        mock_unpack,
        mock_download):
    """Ensures born digital packages trigger the correct methods and arguments."""
    mock_as_auth.return_value = True
    discoverer = PackageDiscoverer(*ARGS)
    download_path = Path(discoverer.ebs_path, f"{discoverer.package_id}.tar.gz")
    package_data = {"origin": "aurora"}
    mock_unpack.return_value = io.BytesIO(), package_data
    package_size = 12345
    mock_package_size.return_value = package_size

    discoverer.run()

    mock_failure_notification.assert_not_called()
    mock_success_notification.assert_called_once_with(package_data, package_size)
    mock_success_cleanup.assert_called_once_with()
    mock_package_size.assert_called_once()
    mock_deliver.assert_not_called()
    mock_unpack.assert_called_once_with(download_path)
    mock_download.assert_called_once_with(download_path)


@patch('src.discover_packages.PackageDiscoverer.download')
@patch('src.discover_packages.PackageDiscoverer.unpack')
@patch('src.discover_packages.PackageDiscoverer.deliver_to_iiif_pipeline')
@patch('src.discover_packages.PackageDiscoverer.get_package_size')
@patch('src.discover_packages.PackageDiscoverer.cleanup_successful_job')
@patch('src.discover_packages.PackageDiscoverer.deliver_success_notification')
@patch('src.discover_packages.PackageDiscoverer.deliver_failure_notification')
@patch('asnake.client.ASnakeClient.authorize')
def test_run_digitized(
        mock_as_auth,
        mock_failure_notification,
        mock_success_notification,
        mock_success_cleanup,
        mock_package_size,
        mock_deliver,
        mock_unpack,
        mock_download):
    """Ensures digitized packages trigger the correct methods and arguments."""
    mock_as_auth.return_value = True
    discoverer = PackageDiscoverer(*ARGS)
    download_path = Path(discoverer.ebs_path, f"{discoverer.package_id}.tar.gz")
    package_data = {"origin": "digitization"}
    mock_unpack.return_value = io.BytesIO(), package_data
    package_size = 12345
    mock_package_size.return_value = package_size

    discoverer.run()

    mock_failure_notification.assert_not_called()
    mock_success_notification.assert_called_once_with(package_data, package_size)
    mock_success_cleanup.assert_called_once_with()
    mock_package_size.assert_called_once()
    mock_deliver.assert_called_once()
    mock_unpack.assert_called_once_with(download_path)
    mock_download.assert_called_once_with(download_path)


@patch('src.discover_packages.PackageDiscoverer.download')
@patch('src.discover_packages.PackageDiscoverer.deliver_failure_notification')
@patch('asnake.client.ASnakeClient.authorize')
def test_run_exception(
        mock_as,
        mock_failure_notification,
        mock_download):
    """Ensures exceptions are handled correctly."""
    mock_as.return_value = True
    discoverer = PackageDiscoverer(*ARGS)
    exception = Exception("Invalid refid.")
    mock_download.side_effect = exception
    discoverer.run()
    mock_failure_notification.assert_called_once_with(exception)


@mock_aws
@patch('src.discover_packages.get_client_with_role')
@patch('asnake.client.ASnakeClient.authorize')
def test_download(mock_as, mock_role):
    mock_as.return_value = True
    discoverer = PackageDiscoverer(*ARGS)
    s3 = boto3.client('s3', region_name='us-east-1')
    mock_role.return_value = s3
    s3.create_bucket(Bucket=discoverer.source_bucket)
    s3.put_object(
        Bucket=discoverer.source_bucket,
        Key=f"{discoverer.package_id}.tar.gz",
        Body='')
    download_path = Path(discoverer.ebs_path, f"{discoverer.package_id}.tar.gz")

    discoverer.download(download_path)
    download_path.is_file()


@mock_aws
@patch('src.discover_packages.PackageDiscoverer.get_as_ref_id')
@patch('asnake.client.ASnakeClient.authorize')
def test_unpack(mock_as, mock_refid):
    """Tests unpacking for both aurora and digitization package."""
    mock_as.return_value = True
    mock_refid.return_value = "123456"
    discoverer = PackageDiscoverer(*ARGS)
    for identifier in ["f78742e5-6af9-4756-a94a-6cd297406d50", "f78742e5-6af9-4756-a94a-6cd297406d51"]:
        discoverer.package_id = identifier
        fixture_path = Path(
            "tests",
            "fixtures",
            "bags",
            f"{identifier}.tar.gz")
        tmp_path = Path(discoverer.ebs_path, f"{discoverer.package_id}.tar.gz")
        copy(fixture_path, tmp_path)
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket=discoverer.assembly_bucket)

        package_filepath, package_data = discoverer.unpack(tmp_path)

        assert isinstance(package_filepath, str)
        with open(Path("tests", "fixtures", "json", f"{identifier}.json"), "r") as df:
            expected_data = json.load(df)
            assert package_data == expected_data
        assert s3.head_object(Bucket=discoverer.assembly_bucket, Key=f"{discoverer.package_id}.tar.gz")

    mock_refid.assert_has_calls([
        call('/repositories/2/archival_objects/1150893'),
        call('/repositories/2/archival_objects/1150893')
    ])


@mock_aws
@patch('asnake.client.ASnakeClient.authorize')
def test_deliver_to_iiif_pipeline(mock_as):
    """Tests binary is correctly moved and POST request is sent with correct data."""
    mock_as.return_value = True
    discoverer = PackageDiscoverer(*ARGS)
    fixture_path = Path(
        "tests",
        "fixtures",
        "bags",
        "f78742e5-6af9-4756-a94a-6cd297406d50.tar.gz")
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket=discoverer.iiif_bucket)
    discoverer.deliver_to_iiif_pipeline(fixture_path)

    assert s3.head_object(
        Bucket=discoverer.iiif_bucket,
        Key=f"{discoverer.package_id}.tar.gz")


@patch('asnake.client.ASnakeClient.authorize')
def test_get_package_size(mock_as):
    mock_as.return_value = True
    fixture_path = fixture_path = Path(
        "tests",
        "fixtures",
        "bags",
        "f78742e5-6af9-4756-a94a-6cd297406d50.tar.gz")
    discoverer = PackageDiscoverer(*ARGS)
    package_size = discoverer.get_package_size(fixture_path)
    assert package_size == 17670


@mock_aws
@patch('src.discover_packages.get_client_with_role')
@patch('asnake.client.ASnakeClient.authorize')
def test_cleanup_successful_job(mock_as, mock_role):
    """Ensures file are cleaned up as expected."""
    mock_as.return_value = True
    discoverer = PackageDiscoverer(*ARGS)
    s3 = boto3.client('s3', region_name='us-east-1')
    mock_role.return_value = s3
    s3.create_bucket(Bucket=discoverer.source_bucket)
    s3.put_object(
        Bucket=discoverer.source_bucket,
        Key=f"{discoverer.package_id}.tar.gz",
        Body='')

    discoverer.cleanup_successful_job()

    with pytest.raises(ClientError) as err:
        s3.head_object(
            Bucket=discoverer.source_bucket,
            Key=f"{discoverer.package_id}.tar.gz",)
    assert '404' in str(err)


@mock_aws
@patch('src.discover_packages.get_client_with_role')
@patch('asnake.client.ASnakeClient.authorize')
def test_deliver_success_notification(mock_as, mock_role):
    """Asserts success messages are delivered as expected."""
    mock_as.return_value = True
    sns = boto3.client('sns', region_name='us-east-1')
    mock_role.return_value = sns
    topic_arn = sns.create_topic(
        Name='my-topic.fifo',
        Attributes={
            "FifoTopic": "true",
            "ContentBasedDeduplication": "true"
        }
    )['TopicArn']
    sqs_conn = boto3.resource("sqs", region_name="us-east-1")
    queue_name = "test-queue.fifo"
    sqs_conn.create_queue(
        QueueName=queue_name,
        Attributes={
            "FifoQueue": "true",
            "ContentBasedDeduplication": "true"
        }
    )
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=f"arn:aws:sqs:us-east-1:{DEFAULT_ACCOUNT_ID}:{queue_name}",
    )

    default_args = ARGS
    default_args[7] = topic_arn
    discoverer = PackageDiscoverer(*default_args)

    package_data = {}
    package_size = 12345
    discoverer.deliver_success_notification(package_data, package_size)

    queue = sqs_conn.get_queue_by_name(QueueName=queue_name)
    messages = queue.receive_messages(MaxNumberOfMessages=1)
    message_body = json.loads(messages[0].body)
    assert message_body['Message'] == json.dumps(package_data)
    assert message_body['MessageAttributes']['outcome']['Value'] == 'SUCCESS'
    assert message_body['MessageAttributes']['package_id']['Value'] == discoverer.package_id
    assert message_body['MessageAttributes']['service']['Value'] == discoverer.service_name
    assert message_body['MessageAttributes']['message']['Value'] == 'Package successfully discovered and downloaded.'
    assert message_body['MessageAttributes']['size']['Value'] == str(package_size)


@mock_aws
@patch('src.discover_packages.get_client_with_role')
@patch('traceback.format_exception')
@patch('asnake.client.ASnakeClient.authorize')
def test_deliver_failure_notification(mock_as, mock_traceback, mock_role):
    """Asserts failure messages are delivered as expected."""
    sns = boto3.client('sns', region_name='us-east-1')
    mock_as.return_value = True
    mock_role.return_value = sns
    topic_arn = sns.create_topic(
        Name='my-topic.fifo',
        Attributes={
            "FifoTopic": "true",
            "ContentBasedDeduplication": "true"
        }
    )['TopicArn']
    sqs_conn = boto3.resource("sqs", region_name="us-east-1")
    queue_name = "test-queue.fifo"
    sqs_conn.create_queue(
        QueueName=queue_name,
        Attributes={
            "FifoQueue": "true",
            "ContentBasedDeduplication": "true"
        }
    )
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=f"arn:aws:sqs:us-east-1:{DEFAULT_ACCOUNT_ID}:{queue_name}",
    )

    default_args = ARGS
    default_args[7] = topic_arn
    discoverer = PackageDiscoverer(*default_args)
    exception_message = "foo"
    exception = Exception(exception_message)
    mock_traceback.return_value = ["baz", "buzz"]

    discoverer.deliver_failure_notification(exception)

    queue = sqs_conn.get_queue_by_name(QueueName=queue_name)
    messages = queue.receive_messages(MaxNumberOfMessages=1)
    message_body = json.loads(messages[0].body)
    assert message_body['Message'] == "baz"
    assert message_body['MessageAttributes']['outcome']['Value'] == 'FAILURE'
    assert message_body['MessageAttributes']['package_id']['Value'] == discoverer.package_id
    assert exception_message in message_body['MessageAttributes']['message']['Value']
