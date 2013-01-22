#!/usr/bin/env python
from boto.s3.key import Key
import boto
import yaml

credentials = yaml.load(open("credentials.yaml"))
key_id = credentials['auth']['key_id']
access_key = credentials['auth']['access_key']

conn = boto.connect_s3(key_id, access_key)
bucket = conn.create_bucket("delfick-s3play")

key = Key(bucket)
key.key = 'blah2'
key.set_contents_from_string("stuff")

from pdb import set_trace; set_trace()

