
#######################################################################################################
# Solution
#######################################################################################################
'''
Major changes:

1. Added check for last update date of shared snapshot and local copy.
2. Local copy is used until shared snapshot is updated.
3. If shared snapshot is updated, old local copy is deleted and new one is created.

These changes ensure that local copy is used until shared snapshot is updated,
which minimizes unnecessary copies and speeds up restore process.
'''
import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
import boto3
import click
import logging

if TYPE_CHECKING:
    from mypy_boto3_rds import RDSClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RDS:
    RDS_KMS = "arn:aws:kms:eu-central-1:123456789012:alias/shared_kms"

    def __init__(self, aws_profile_name=None):
        session = boto3.session.Session(profile_name=aws_profile_name)
        self.rds_client: RDSClient = session.client('rds')

    def restore_instance_from_shared(self, snapshot_identifier, target_db_identifier, config_from_db_identifier) -> str:
        logger.info(f"Restoring RDS instance from snapshot '{snapshot_identifier}'")
        
        # Getting a shared snapshot
        snapshots = self.rds_client.describe_db_snapshots(SnapshotType="shared", IncludeShared=True)['DBSnapshots']
        available_snapshots = [s for s in snapshots if s['Status'] == 'available' and s['DBInstanceIdentifier'] == snapshot_identifier]
        newest_snapshot = max(available_snapshots, key=lambda s: s['SnapshotCreateTime'])

        # Getting local snapshots
        local_snapshots = self.rds_client.describe_db_snapshots(DBInstanceIdentifier=snapshot_identifier, SnapshotType="manual")['DBSnapshots']
        if local_snapshots:
            newest_local_snapshot = max(local_snapshots, key=lambda s: s['SnapshotCreateTime'])
            local_snapshot_time = newest_local_snapshot['SnapshotCreateTime']
        else:
            local_snapshot_time = None

        # Shared snapshot creation time
        shared_snapshot_time = newest_snapshot['SnapshotCreateTime']

        # Checking if the local copy needs to be updated
        if local_snapshot_time is None or shared_snapshot_time > local_snapshot_time:
            logger.info("Shared snapshot is newer than local snapshot or local snapshot does not exist. Creating a new local copy.")
            self.delete_old_snapshots(self.rds_client, snapshot_identifier)
            try:
                newest_snapshot_copy = self.rds_client.copy_db_snapshot(
                    SourceDBSnapshotIdentifier=newest_snapshot['DBSnapshotArn'],
                    TargetDBSnapshotIdentifier=f"{newest_snapshot['DBInstanceIdentifier']}-copy",
                    KmsKeyId=RDS.RDS_KMS,
                )['DBSnapshot']
                self.wait_snapshot(self.rds_client, newest_snapshot_copy['DBSnapshotIdentifier'], message="Waiting for RDS snapshot copy")
            except self.rds_client.exceptions.DBSnapshotAlreadyExistsFault:
                logger.info("RDS Snapshot local copy already exists -- skipping")
                newest_snapshot_copy = self.rds_client.describe_db_snapshots(DBInstanceIdentifier=snapshot_identifier, SnapshotType="manual")['DBSnapshots'][0]
        else:
            logger.info("Using existing local snapshot copy.")
            newest_snapshot_copy = newest_local_snapshot

        # Get the current configuration of the RDS instance. (Restore RDS instance)
        current_instance = self.rds_client.describe_db_instances(DBInstanceIdentifier=config_from_db_identifier)['DBInstances'][0]
        try:
            self.rds_client.restore_db_instance_from_db_snapshot(
                DBInstanceIdentifier=target_db_identifier,
                DBSnapshotIdentifier=newest_snapshot_copy['DBSnapshotIdentifier'],
                DBSubnetGroupName=current_instance['DBSubnetGroup']['DBSubnetGroupName'],
                DBInstanceClass=current_instance['DBInstanceClass'],
            )
        except self.rds_client.exceptions.DBInstanceAlreadyExistsFault:
            logger.info("RDS Instance already exists -- using it")
            instance = self.rds_client.describe_db_instances(DBInstanceIdentifier=target_db_identifier)
            return instance['DBInstances'][0]['Endpoint']['Address']
        instance = self.wait_db(self.rds_client, target_db_identifier)
        return instance['DBInstances'][0]['Endpoint']['Address']

    def destroy_instance(self, db_identifier):
        logger.info(f"Destroying RDS instance '{db_identifier}'")
        try:
            self.rds_client.delete_db_instance(
                DBInstanceIdentifier=db_identifier,
                SkipFinalSnapshot=True,
                DeleteAutomatedBackups=True,
            )
        except self.rds_client.exceptions.DBInstanceNotFoundFault:
            logger.info(f"RDS instance with identifier {db_identifier} is not found -- nothing to delete")

    @staticmethod
    def delete_old_snapshots(client, snapshot_identifier):
        snapshots = client.describe_db_snapshots(
            DBInstanceIdentifier=snapshot_identifier,
            SnapshotType='manual'
        )['DBSnapshots']
        for snapshot in snapshots:
            if snapshot_time := snapshot.get('SnapshotCreateTime', None):
                retention_period = timedelta(days=7)
                if datetime.now(snapshot_time.tzinfo) - snapshot_time > retention_period:
                    snapshot_name = snapshot['DBSnapshotIdentifier']
                    client.delete_db_snapshot(DBSnapshotIdentifier=snapshot_name)
                    logger.info(f"Deleted old snapshot {snapshot_name}")

    @staticmethod
    def wait_db(client, identifier, message="Waiting for RDS instance to be stable"):
        sleep_time = 5
        good_statuses = ['available', 'applying']
        with click.progressbar(range(1, 300), label=message) as bar:
            for x in bar:
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
                if x > (60 / sleep_time):
                    anonymized_snapshot = client.describe_db_snapshots(DBSnapshotIdentifier=identifier)
                    if anonymized_snapshot['DBSnapshots'][0]['Status'] in good_statuses:
                        return anonymized_snapshot
                time.sleep(sleep_time)
            else:
                raise RDSException(f"The snapshot {identifier} has timed out to be stable")


class RDSException(Exception):
    pass