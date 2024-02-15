# Get a list of unattached EBS Volumes

import boto3
import csv
from datetime import datetime
import os

# General variables
dry_run = os.environ["DRY_RUN"]
date_str = datetime.now().strftime('%Y-%m-%d')

# Select regions for find unattached volumes
regions = ["us-east-1", "us-east-2"]

# csv output file settings
export_to_file = True
output_dir = './'



# Get a list of unattached EBS Volumes
def get_unattached_volumes (ec2_client):
    
    unattached_volumes = ec2_client.describe_volumes(
        Filters=[
            {
                'Name': 'status',
                'Values': [
                    'available',
                ]
            },
        ],
    )
    return unattached_volumes


# If dry_run is False, take a snapshot of the unattached volumes and tag them with the current date
def take_volume_snapshot (ec2_client, unattached_volumes, region):

    if dry_run is False:

        for volume in unattached_volumes['Volumes']:
            print(f"Creating snapshot of {volume['VolumeId']} - {region}")
            Tags = [
                {'Key': 'Description',
                    'Value': 'Unattached Volume Snapshot of ' + volume['VolumeId']}
            ]
            ec2_client.create_snapshot(
                VolumeId=volume['VolumeId'],
                Description='Created by SRE Python script',
                TagSpecifications=[
                    {
                        'ResourceType': 'snapshot',
                        'Tags': Tags
                    },
                ],
            )
    elif dry_run:

        for volume in unattached_volumes['Volumes']:
            print(f"Dry run - Creating snapshot of {volume['VolumeId']} - {region}")           


# If dry_run is False, delete the unattached volumes
def delete_unattached_volumes (ec2_client, unattached_volumes, region):
    if dry_run is False:

        for volume in unattached_volumes['Volumes']:
            print(f"Deleting {volume['VolumeId']} - {region}") 
            ec2_client.delete_volume(
                VolumeId=volume['VolumeId']
            )
    elif dry_run:

        for volume in unattached_volumes['Volumes']:
            print(f"Dry run - Deleting {volume['VolumeId']} - {region}")        


# If dry_run is False, delete all but the most recent snapshot of the unattached volumes
def delete_most_recent_snapshots (ec2_client, unattached_volumes, region):

    if dry_run is False:

        for volume in unattached_volumes['Volumes']:
            print(f"Deleting snapshots of {volume['VolumeId']} - {region}")
            snapshots = ec2_client.describe_snapshots(
                Filters=[
                    {
                        'Name': 'volume-id',
                        'Values': [
                            volume['VolumeId'],
                        ]
                    },
                ],
            )
            snapshots_sorted = sorted(
                snapshots['Snapshots'], key=lambda x: x['StartTime'], reverse=True)
            for snapshot in snapshots_sorted[1:]:
                print(f"Deleting snapshot {snapshot['SnapshotId']} - {region}")
                ec2_client.delete_snapshot(
                    SnapshotId=snapshot['SnapshotId']
                )
    elif dry_run:

        for volume in unattached_volumes['Volumes']:
            print(f"Dry run - Deleting snapshots of {volume['VolumeId']} - {region}")
            snapshots = ec2_client.describe_snapshots(
                Filters=[
                    {
                        'Name': 'volume-id',
                        'Values': [
                            volume['VolumeId'],
                        ]
                    },
                ],
            )
            snapshots_sorted = sorted(
                snapshots['Snapshots'], key=lambda x: x['StartTime'], reverse=True)
            for snapshot in snapshots_sorted[1:]:
                print(f"Dry run - Deleting snapshot {snapshot['SnapshotId']} - {region}")

# If export_to_file = True write data to a csv file with columns VolumeId, Size, AvailabilityZone (region), CreateTime
def export_unattached_volumes_to_csv (regions):
    
    iam = boto3.client('iam')  
    account_aliases = iam.list_account_aliases()['AccountAliases']
    account_alias = account_aliases[0] if account_aliases else 'unknown'
    output_dir = './'
    output_file = f'{account_alias}_unattached_ebs_{date_str}.csv'
    output_path = os.path.join(output_dir, output_file) 



    if export_to_file is True:
        with open(output_path, 'w') as f:
            f.write('VolumeId,Size,AvailabilityZone,CreateTime\n')
            for region in regions: 
                ec2_client = boto3.client('ec2', region_name=region)
                unattached_volumes = get_unattached_volumes (ec2_client)
                for volume in unattached_volumes['Volumes']:
                        f.write(
                            f"{volume['VolumeId']},{volume['Size']},{volume['AvailabilityZone']},{volume['CreateTime']}\n")

def main ():
    export_unattached_volumes_to_csv (regions)

    for region in regions:
        ec2_client = boto3.client('ec2', region_name=region)
        unattached_volumes = get_unattached_volumes (ec2_client)

        take_volume_snapshot (ec2_client, unattached_volumes, region)
        delete_unattached_volumes (ec2_client, unattached_volumes, region)
        delete_most_recent_snapshots (ec2_client, unattached_volumes, region)
       
    


if __name__ == "__main__":
    main()
