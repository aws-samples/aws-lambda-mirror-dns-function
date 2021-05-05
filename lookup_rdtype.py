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
# lookup_rdtype.py
#
# Mapping of dnspython record types to traditional DNS record types so that
# AWS Route 53 API call will understand the request.

record_type = {
    0: 'NONE',
    1: 'A',
    2: 'NS',
    3: 'MD',
    4: 'MF',
    5: 'CNAME',
    6: 'SOA',
    7: 'MB',
    8: 'MG',
    9: 'MR',
    10: 'NULL',
    11: 'WKS',
    12: 'PTR',
    13: 'HINFO',
    14: 'MINFO',
    15: 'MX',
    16: 'TXT',
    17: 'RP',
    18: 'AFSDB',
    19: 'X25',
    20: 'ISDN',
    21: 'RT',
    22: 'NSAP',
    23: 'NSAP_PTR',
    24: 'SIG',
    25: 'KEY',
    26: 'PX',
    27: 'GPOS',
    28: 'AAAA',
    29: 'LOC',
    30: 'NXT',
    33: 'SRV',
    35: 'NAPTR',
    36: 'KX',
    37: 'CERT',
    38: 'A6',
    39: 'DNAME',
    41: 'OPT',
    42: 'APL',
    43: 'DS',
    44: 'SSHFP',
    45: 'IPSECKEY',
    46: 'RRSIG',
    47: 'NSEC',
    48: 'DNSKEY',
    49: 'DHCID',
    50: 'NSEC3',
    51: 'NSEC3PARAM',
    52: 'TLSA',
    55: 'HIP',
    59: 'CDS',
    60: 'CDNSKEY',
    62: 'CSYNC',
    99: 'SPF',
    103: 'UNSPEC',
    108: 'EUI48',
    109: 'EUI64',
    249: 'TKEY',
    250: 'TSIG',
    251: 'IXFR',
    252: 'AXFR',
    253: 'MAILB',
    254: 'MAILA',
    255: 'ANY',
    256: 'URI',
    257: 'CAA',
    32768: 'TA',
    32769: 'DLV'
}


def recmap(rdtype):
    for key, value in record_type.items():
        if key == rdtype:
            return value
