import boto3
from datetime import datetime, timedelta

def lambda_handler(event, context):
    ec2 = boto3.client('ec2')

    # Get the current date and time
    now = datetime.utcnow()

    # Calculate the date 30 days ago
    days_ago_30 = now - timedelta(days=30)

    # Get all EBS snapshots
    response = ec2.describe_snapshots(OwnerIds=['self'])

    # Get all active EC2 instance IDs
    instances_response = ec2.describe_instances(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    active_instance_ids = set()

    for reservation in instances_response['Reservations']:
        for instance in reservation['Instances']:
            active_instance_ids.add(instance['InstanceId'])

    # Iterate through each snapshot
    for snapshot in response['Snapshots']:
        snapshot_id = snapshot['SnapshotId']
        start_time = snapshot['StartTime'].replace(tzinfo=None)  # Remove timezone info for comparison
        volume_id = snapshot.get('VolumeId')

        # Check if the snapshot is older than 30 days
        if start_time < days_ago_30:
            if not volume_id:
                # Delete the snapshot if it's not attached to any volume
                ec2.delete_snapshot(SnapshotId=snapshot_id)
                print(f"Deleted EBS snapshot {snapshot_id} as it was older than 30 days and not attached to any volume.")
            else:
                # Check if the volume still exists
                try:
                    volume_response = ec2.describe_volumes(VolumeIds=[volume_id])
                    # If the volume has no attachments, or it's not attached to any running instance, delete the snapshot
                    if not volume_response['Volumes'][0]['Attachments']:
                        ec2.delete_snapshot(SnapshotId=snapshot_id)
                        print(f"Deleted EBS snapshot {snapshot_id} as it was older than 30 days and the volume is not attached to any running instance.")
                    else:
                        # Check if any of the volume's attachments are linked to a running instance
                        attached_instance_ids = {attachment['InstanceId'] for attachment in volume_response['Volumes'][0]['Attachments']}
                        if not attached_instance_ids.intersection(active_instance_ids):
                            ec2.delete_snapshot(SnapshotId=snapshot_id)
                            print(f"Deleted EBS snapshot {snapshot_id} as it was older than 30 days and not attached to a running instance.")
                except ec2.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == 'InvalidVolume.NotFound':
                        # The volume associated with the snapshot is not found (it might have been deleted)
                        ec2.delete_snapshot(SnapshotId=snapshot_id)
                        print(f"Deleted EBS snapshot {snapshot_id} as it was older than 30 days and its associated volume was not found.")
