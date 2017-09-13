# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not
# use this file except in compliance with the License. A copy of the License is
# located at
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

# -*- coding: utf-8 -*-
#
# lambda_function.py
#
# AWS Lambda function to mirror an on-premises DNS to Route 53 private hosted zone
# Supports both forward and reverse zones for replications to Route 53.

# These imports are bundled local to the lambda function 
import dns.query
import dns.zone
import lookup_rdtype
from dns.rdataclass import *
from dns.rdatatype import *

# libraries that are available on Lambda
import os
import sys
import boto3

# If you need to use a proxy server to access the Internet then hard code it 
# the details below, otherwise comment out or remove.
#os.environ["http_proxy"] = "10.10.10.10:3128"  # My on-premises proxy server
#os.environ["https_proxy"] = "10.10.10.10:3128"
#os.environ["no_proxy"] = "169.254.169.254"  # Don't proxy for meta-data service as Lambda  needs to get IAM credentials

# setup the boto3 client to talk to AWS APIs
route53 = boto3.client('route53')


# Function to create, update, delete records in Route 53
def update_resource_record(zone_id, host_name, hosted_zone_name, rectype, changerec, ttl, action):
    if not (rectype == 'NS' and host_name == '@'):
        print 'Updating as %s for %s record %s TTL %s in zone %s with %s ' % (
            action, rectype, host_name, ttl, hosted_zone_name, changerec)
        if rectype != 'SOA':
            if host_name == '@':
                host_name = ''
            elif host_name[-1] != '.':
                host_name += '.'
                # Make Route 53 change record set API call
        dns_changes = {
            'Comment': 'Managed by Lambda Mirror DNS',
            'Changes': [
                {
                    'Action': action,
                    'ResourceRecordSet': {
                        'Name': host_name + hosted_zone_name,
                        'Type': rectype,
                        'ResourceRecords': [],
                        'TTL': ttl
                    }
                }
            ]
        }


        for value in changerec:  # Build the recordset
            if (rectype != 'CNAME' and rectype != 'SRV' and rectype != 'MX' and rectype!= 'NS') or (str(value)[-1] == '.'):
                dns_changes['Changes'][0]['ResourceRecordSet']['ResourceRecords'].append({'Value': str(value)})
            else:
                dns_changes['Changes'][0]['ResourceRecordSet']['ResourceRecords'].append({'Value': str(value) + '.' + hosted_zone_name + '.'})

        try:  # Submit API request to Route 53
            route53.change_resource_record_sets(HostedZoneId=zone_id, ChangeBatch=dns_changes)
        except BaseException as e:
            print e
            sys.exit('ERROR: Unable to update zone %s' % hosted_zone_name)
        return True


# Perform a diff against the two zones and return difference set
def diff_zones(zone1, zone2, ignore_ttl):
    differences = []
    for node in zone1:     # Process delete for records which are not in the new zone
        node1 = zone1.get_node(node)
        node2 = zone2.get_node(node)
        if not node2:
            for record1 in node1:
                changerec = []
                for value1 in record1:
                    changerec.append(value1)
                change = (str(node), record1.rdtype, changerec, record1.ttl, 'DELETE')
                if change not in differences:
                    differences.append(change)
        else:
            for record1 in node1:
                record2 = node2.get_rdataset(record1.rdclass, record1.rdtype)
                if record1 != record2:  # update record to new zone
                    changerec = []
                    if record2:
                        action = 'UPSERT'
                        for value2 in record2:
                            changerec.append(value2)
                    else:
                        action = 'DELETE'
                        for value1 in record1:
                            changerec.append(value1)
                    change = (str(node), record1.rdtype, changerec, record1.ttl, action)
                    if change and change not in differences:
                        differences.append(change)

    for node in zone2:  # Process records in master zone
        node1 = zone1.get_node(node)
        node2 = zone2.get_node(node)
        if not node1:  # Entry doesn't exist, lets create it
            for record2 in node2:
                changerec = []
                for value2 in record2:
                    changerec.append(value2)
                    change = (str(node), record2.rdtype, changerec, record2.ttl, 'CREATE')
                    if change and change not in differences:
                        differences.append(change)

        else:  # Check for updates to record for existing entries
            for record2 in node2:
                record1 = node1.get_rdataset(record2.rdclass, record2.rdtype)
                if record2.rdtype == dns.rdatatype.SOA:
                    continue
                elif not record1:  # Create new record
                    changerec = []
                    for value2 in record2:
                        changerec.append(value2)
                        change = (str(node), record2.rdtype, changerec, record2.ttl, 'UPSERT')
                        if change and change not in differences:
                            differences.append(change)
                elif record1 != record2:  # update record to new zone
                    changerec = []
                    for value2 in record2:
                        changerec.append(value2)
                    change = (str(node), record2.rdtype, changerec, record2.ttl, 'UPSERT')
                    if change and change not in differences:
                        differences.append(change)

                if record2.rdtype == dns.rdatatype.SOA or not record1:
                    continue
                elif not ignore_ttl and record2.ttl != record1.ttl:  # Check if the TTL has been updated
                    changerec = []
                    for value2 in record2:
                        changerec.append(value2)
                    change = (str(node), record2.rdtype, changerec, record2.ttl, 'UPSERT')
                    if change and change not in differences:
                        differences.append(change)
                elif record2.ttl != record1.ttl:
                    print 'Ignoring TTL update for %s' % node

    return differences


# Main Handler for lambda function
def lambda_handler(event, context):
    # Setup configuration based on JSON formatted event data
    try:
        domain_name = event['Domain']
        master_ip = event['MasterDns']
        route53_zone_id = event['ZoneId']
        if event['IgnoreTTL'] == 'True':
            ignore_ttl = True  # Ignore TTL changes in records
        else:
            ignore_ttl = False  # Update records even if the change is just the TTL
    except BaseException as e:
        print 'Error in setting up the environment, exiting now (%s) ' % e
        sys.exit('ERROR: check JSON file is complete:', event)

    # Transfer the master zone file from the DNS server via AXFR
    print 'Transferring zone %s from server %s ' % (domain_name, master_ip)
    try:
        master_zone = dns.zone.from_xfr(dns.query.xfr(master_ip, domain_name))
    except BaseException as e:
        print 'Problem transferring zone'
        print e
        sys.exit('ERROR: Unable to retrieve zone %s from %s' % (domain_name, master_ip))

    soa = master_zone.get_rdataset('@', 'SOA')
    serial = soa[0].serial  # What's the current zone version on-prem

    # Read the zone from Route 53 via API and populate into zone object
    vpc_zone = dns.zone.Zone(origin=domain_name)
    print 'Getting VPC SOA serial from Route 53'  # Get the SOA from Route 53 by API to avoid getting stale records
    try:
        vpc_recordset = list(route53.get_paginator('list_resource_record_sets').paginate(HostedZoneId=route53_zone_id).search('ResourceRecordSets'))
        for record in vpc_recordset:
            # Change the record name so that it doesn't have the domain name appended
            recordname = record['Name'].replace(domain_name + '.', '')
            if recordname == '':
                recordname = '@'
            else:
                recordname = recordname.rstrip('.')
            rdataset = vpc_zone.find_rdataset(recordname, rdtype=str(record['Type']), create=True)
            for value in record['ResourceRecords']:
                rdata = dns.rdata.from_text(1, rdataset.rdtype, value['Value'].replace(domain_name + '.', ''))
                rdataset.add(rdata, ttl=int(record['TTL']))
    except BaseException as e:
        print e
        sys.exit('ERROR: Unable to retrieve VPC Zone via API (%s)' % e)

    # Compare the master and VPC Route 53 zone file
    vpc_soa = vpc_zone.get_rdataset('@', 'SOA')
    vpc_serial = vpc_soa[0].serial
    if not (vpc_serial > serial):
        print 'Comparing SOA serial %s with %s ' % (vpc_serial, serial)
        differences = diff_zones(vpc_zone, master_zone, ignore_ttl)

        for host, rdtype, record, ttl, action in differences:
            if rdtype != dns.rdatatype.SOA:
                update_resource_record(route53_zone_id, host, domain_name, lookup_rdtype.recmap(rdtype), record, ttl,
                                       action)

        # Update the VPC SOA to reflect the version just processed
        vpc_soa[0].serial = serial
        try:
            soarecord = [str(vpc_soa[0])]
            update_resource_record(route53_zone_id, '', domain_name, 'SOA', soarecord, vpc_soa[0].minimum, 'UPSERT')
        except BaseException as e:
            print e
            sys.exit('ERROR: Failed to update SOA to %s on Route 53 VPC Zone' % str(serial))
    else:
        sys.exit('ERROR: Route 53 VPC serial %s for domain %s is greater than existing serial %s' % (str(vpc_serial), domain_name, str(serial)))

    return 'SUCCESS: %s mirrored to Route 53 VPC serial %s' % (domain_name, str(serial))
