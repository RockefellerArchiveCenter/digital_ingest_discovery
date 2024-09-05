import json
from pathlib import Path

import boto3
import rac_schema_validator
from aws_assume_role_lib import assume_role


def get_client_with_role(self, resource, role_arn):
    """Gets Boto3 client which authenticates with a specific IAM role."""
    session = boto3.Session()
    assumed_role_session = assume_role(session, role_arn)
    return assumed_role_session.client(resource)


def validate_bag_data(bag_data, schema_name):
    """Validates bag data against RAC schemas."""
    base_file = open(Path('rac_schemas', 'schemas', 'base.json'), 'r')
    base_schema = json.load(base_file)
    base_file.close()
    with open(Path('rac_schemas', 'schemas', schema_name), 'r') as object_file:
        object_schema = json.load(object_file)
        return rac_schema_validator.is_valid(bag_data, object_schema, base_schema)
