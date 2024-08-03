'''
Inside this function, a copy of the shared snapshot is made locally and an RDS instance is deployed.
If the copy is more than 7 days old, it is deleted and copied again from the shared.
A different approach is needed, we use the local copy until the shared version is updated.
If the shared version is updated, we delete the local copy and make a new copy from the shared.
'''
######################################################################################################
# The solution in the "rds_new.py" file
######################################################################################################


import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import boto3
import click

from logger import Logger

if TYPE_CHECKING:
    from mypy_boto3_rds import RDSClient


class RDS:
    RDS_KMS = "arn:aws:kms:eu-central-1:123456789012:alias/shared_kms"

    def __init__(self, aws_profile_name=None):
        session = boto3.session.Session(profile_name=aws_profile_name)
        self.rds_client: RDSClient = session.client('rds')

    def restore_instance_from_shared(self, snapshot_identifier, target_db_identifier, config_from_db_identifier) -> str:
        Logger.info(f"Restoring RDS instance from snapshot '{snapshot_identifier}'")
        # DBInstanceIdentifier (string)
        # Constraints:
        # If supplied, must match the identifier of an existing DBInstance.
        snapshots = self.rds_client.describe_db_snapshots(SnapshotType="shared", IncludeShared=True)['DBSnapshots']
        # Filter for 'available' snapshots after retrieval because it could be that it conflicts with creating backup
        available_snapshots = [s for s in snapshots if s['Status'] == 'available' and s['DBInstanceIdentifier'] == snapshot_identifier]
        newest_snapshot = max(available_snapshots, key=lambda s: s['SnapshotCreateTime'])

        # Check if local manual copy of anon DB is old and need to be refreshed
        RDS.delete_old_snapshots(self.rds_client, newest_snapshot['DBInstanceIdentifier'])
        # Do a local manual snapshot copy from the shared to deal with encrypted
        try:
            newest_snapshot_copy = self.rds_client.copy_db_snapshot(
                SourceDBSnapshotIdentifier=newest_snapshot['DBSnapshotArn'],
                TargetDBSnapshotIdentifier=newest_snapshot['DBInstanceIdentifier'],
                KmsKeyId=RDS.RDS_KMS,
            )['DBSnapshot']
            RDS.wait_snapshot(self.rds_client, newest_snapshot_copy['DBSnapshotIdentifier'], message="Waiting for RDS snapshot copy")
        except self.rds_client.exceptions.DBSnapshotAlreadyExistsFault:
            Logger.info("RDS Snapshot local copy already exists -- skipping")
            newest_snapshot_copy = self.rds_client.describe_db_snapshots(DBInstanceIdentifier=snapshot_identifier, SnapshotType="manual")['DBSnapshots'][0]

        # little hack, we take data from current instance and copy most configuration to the anonymization
        current_instance = self.rds_client.describe_db_instances(DBInstanceIdentifier=config_from_db_identifier)['DBInstances'][0]
        try:
            self.rds_client.restore_db_instance_from_db_snapshot(
                DBInstanceIdentifier=target_db_identifier,
                DBSnapshotIdentifier=newest_snapshot_copy['DBSnapshotIdentifier'],
                DBSubnetGroupName=current_instance['DBSubnetGroup']['DBSubnetGroupName'],
                DBInstanceClass=current_instance['DBInstanceClass'],
            )
        except self.rds_client.exceptions.DBInstanceAlreadyExistsFault:
            Logger.info("RDS Instance already exists -- using it")
            instance = self.rds_client.describe_db_instances(DBInstanceIdentifier=target_db_identifier)
            return instance['DBInstances'][0]['Endpoint']['Address']
        instance = RDS.wait_db(self.rds_client, target_db_identifier)
        return instance['DBInstances'][0]['Endpoint']['Address']

    def destroy_instance(self, db_identifier):
        Logger.info(f"Destroying RDS instance '{db_identifier}'")
        try:
            self.rds_client.delete_db_instance(
                DBInstanceIdentifier=db_identifier,
                SkipFinalSnapshot=True,
                DeleteAutomatedBackups=True,
            )
        except self.rds_client.exceptions.DBInstanceNotFoundFault:
            Logger.info(f"RDS instance with identifier {db_identifier} is not found -- nothing to delete")

    @staticmethod
    def delete_old_snapshots(client, snapshot_identifier):
        snapshots = client.describe_db_snapshots(
            DBInstanceIdentifier=snapshot_identifier,
            SnapshotType='manual'
        )['DBSnapshots']
        for snapshot in snapshots:
            # don't run on snapshot that don't have CreatedTime, they are still creating
            if snapshot_time := snapshot.get('SnapshotCreateTime', None):
                retention_period = timedelta(days=7)
                if datetime.now(snapshot_time.tzinfo) - snapshot_time > retention_period:
                    snapshot_name = snapshot['DBSnapshotIdentifier']
                    client.delete_db_snapshot(DBSnapshotIdentifier=snapshot_name)
                    Logger.info(f"Deleted old snapshot {snapshot_name}")

    @staticmethod
    def wait_db(client, identifier, message="Waiting for RDS instance to be stable"):
        sleep_time = 5
        good_statuses = ['available', 'applying']
        with click.progressbar(range(1, 300), label=message) as bar:
            for x in bar:
                # try after 2 mins, no need to try before that
                if x > (60 / sleep_time):
                    anonymized_instance = client.describe_db_instances(DBInstanceIdentifier=identifier)
                    if anonymized_instance['DBInstances'][0]['DBInstanceStatus'] in good_statuses:
                        return anonymized_instance
                time.sleep(sleep_time)
            else:
                raise RDSException(f"The instance {identifier} is not in status 'available'")

    @staticmethod
    def wait_snapshot(client, identifier, message="Waiting for RDS snapshot to be available"):
        sleep_time = 5
        good_statuses = ['available']
        with click.progressbar(range(1, 300), label=message) as bar:
            for x in bar:
                # try after 2 mins, no need to try before that
                if x > (60 / sleep_time):
                    anonymized_snapshot = client.describe_db_snapshots(DBSnapshotIdentifier=identifier)
                    if anonymized_snapshot['DBSnapshots'][0]['Status'] in good_statuses:
                        return anonymized_snapshot
                time.sleep(sleep_time)
            else:
                raise RDSException(f"The snapshot {identifier} has timeouted to be stable")


class RDSException(Exception):
    pass