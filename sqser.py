#!/usr/bin/env python
"""
    Test script that uploads random data to sqs
    Any data that is too big for sqs goes to s3 instead
    Then it retrieves all the things from sqs/s3 and makes sure they are deleted
"""

from gevent import monkey
monkey.patch_all()

from boto.exception import S3ResponseError
from boto.sqs.message import RawMessage
from collections import namedtuple
from gevent.pool import Pool
from boto.s3.key import Key
import argparse
import random
import base64
import gevent
import time
import boto
import yaml
import uuid
import json
import sys
import os

import logging
log = logging.getLogger("sqsplay")

Payload = namedtuple('Payload', ['payload', 'sqs_result', 's3_key', 'successful', 'discard'])
"""Holds result from sqs, s3 key; and the payload data itself"""

class NoKey(Exception):
    """For when an sqs message references an s3 key that don't exist"""
    pass

class BadKey(Exception):
    """For when we can't read an s3 key for some non 404 reason"""
    pass

class NoArg(object):
    """Default for an argument that is optional"""
    pass

########################
###   SQS
########################

def sqs_write(data, sqs_queue, bucket):
    """
        Write a message to an sqs queue
        If the message is too big for the queue, then write to an s3 bucket instead
    """
    key = ''
    where = 'sqs'

    m = RawMessage()
    message = {'what':3, 'message':base64.b64encode(data)}
    body = json.dumps(message)
    if len(body) >= 65536:
        key = s3_write(bucket, body)
        body = json.dumps({'s3':key})
        where = 's3 '

    m.set_body(body)
    sqs_queue.write(m)
    log.info("Added to %s%s", where, key)

def sqs_read(sqs_queue, bucket):
    """
        Read a message from sqs and json.loads the body
        If the body is a pointer to s3, then get from there and json.loads that
    """
    results = sqs_queue.get_messages(num_messages=10)
    if results:
        log.info('Got %s', len(results))

    for index, sqs_result in enumerate(results):
        s3_key = None
        payload = None
        discard = False
        successful = True

        try:
            payload = json.loads(sqs_result.get_body())
        except ValueError:
            discard = True
            successful = False

        if successful:
            s3_key = payload.get('s3')
            if s3_key:
                discard, successful, payload = safe_s3_read(bucket, s3_key)

        yield Payload(payload, sqs_result, s3_key, successful, discard)

########################
###   S3
########################

def s3_write(bucket, data):
    """Write data to an s3 bucket, use a uuid as the key"""
    key = Key(bucket)
    key.key = str(uuid.uuid1())
    key.set_contents_from_string(data)
    return key.key

def s3_read(bucket, key_name, attempts=10):
    """Read and return the contents from a particular key in the bucket"""
    key = Key(bucket)
    key.key = key_name
    for i in range(attempts):
        try:
            body = key.get_contents_as_string()
            if not body:
                continue
            return body
        except S3ResponseError as e:
            if i == attempts-1:
                if e.status == 404:
                    raise NoKey(key.key, e)
                else:
                    raise BadKey(key.key, e)
            time.sleep(0.5)

def safe_s3_read(bucket, s3_key):
    """
        Read from s3 and return (discard, successful, payload)
        Where discard says the message should be discarded regardless of successful
        Successful says whether we got data from the s3 successfully
        payload is the json.loads of the data we got from s3
    """
    payload = None
    discard = True
    successful = False

    try:
        read = s3_read(bucket, s3_key)
    except (NoKey, BadKey):
        pass
    else:
        if read is not None:
            try:
                payload = json.loads(read)
                discard = False
                successful = True
            except Exception as e:
                log.error("Bad Payload:: '%s' -> %s", read, e)
                discard = True
                successful = False

    return discard, successful, payload

def s3_delete_all(bucket):
    """Delete evrything in an s3 bucket"""
    keys = bucket.get_all_keys()
    bucket.delete_keys(keys)

########################
###   POOLS
########################

class SingleItemPool(object):
    """Pool that spawns the action for each item that comes through"""
    def __init__(self, limit, action, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.action = action
        self.joining = False
        self.pool = Pool(limit)

    def add_handler(self, item=NoArg):
        """
            Spawn the action with the provided item
            Also use the args and kwargs from __init__
            And don't pass in the item if it wasn't specified
        """
        if item is NoArg:
            self.pool.spawn(self.action, *self.args, **self.kwargs)
        else:
            self.pool.spawn(self.action, item, *self.args, **self.kwargs)

    def add_handlers(self, number, item=NoArg):
        """Add many handlers"""
        for i in range(number):
            self.add_handler(item)

    def join(self):
        """Wait for the spawned greenlets to finish"""
        self.joining = True
        self.pool.join()
        self.joining = False

    def process(self, items, join=True):
        """Add handlers for some items and optionally join"""
        for item in items:
            self.add_handler(item)

        if join:
            self.join()

class BulkItemsPool(SingleItemPool):
    """Pool that only spawns the action for self.every items"""
    def __init__(self, limit, every, action, *args, **kwargs):
        self.buf = []
        self.every = every
        super(BulkItemsPool, self).__init__(limit, action, *args, **kwargs)

    def add_handler(self, item):
        """We create a buffer of items and only spawn when we have greater than self.every of them"""
        self.buf.append(item)
        if len(self.buf) > self.every:
            self.spawn()

    def join(self):
        """Make sure we clear out any existing self.buf items before joining"""
        if self.buf:
            self.spawn()
        super(BulkItemsPool, self).join()

    def spawn(self):
        """Spawn an action for everything in self.buf and clear self.buf"""
        items = [item for item in self.buf]
        self.buf = []
        super(BulkItemsPool, self).add_handler(items)

########################
###   APPLICATION
########################

def make_parser():
    """Make argumentparser for getting things from user"""
    parser = argparse.ArgumentParser(description="Random sqs/s3 play script")
    parser.add_argument("-q", '--sqs-queue'
        , help = "the name of the sqs queue to give data to and read from"
        , required = True
        )
    parser.add_argument("-b", '--s3-bucket'
        , help = "The name of the s3 bucket to use for large messages"
        , required = True
        )
    return parser

def setup_logging():
    """Setup the logging"""
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("[%(levelname)s || %(asctime)s] %(message)s"))
    log.addHandler(handler)
    log.setLevel(logging.INFO)

if __name__ == '__main__':
    # Get the name of the queue and s3 bucket from the user
    # And setup the logging
    parser = make_parser()
    args = parser.parse_args()
    setup_logging()

    ########################
    ###   SETUP
    ########################

    # Some counts
    counts = dict(s3_deleted_count = 0, sqs_deleted_count = 0, processed = 0, received = 0)

    # Get me some credentials
    credentials = yaml.load(open("credentials.yaml"))
    key_id = credentials['auth']['key_id']
    access_key = credentials['auth']['access_key']

    log.info("Connecting to amazon")
    s3_conn = boto.connect_s3(key_id, access_key, is_secure=False)
    sqs_conn = boto.connect_sqs(key_id, access_key, is_secure=False)

    log.info("Creating sqs queue")
    sqs_queue = sqs_conn.create_queue(args.sqs_queue)
    sqs_queue.set_message_class(RawMessage)

    log.info("Creating s3 bucket")
    s3_bucket = s3_conn.create_bucket(args.s3_bucket)

    log.info("Generating some data")
    start = time.time()
    datas = [os.urandom(random.randrange(10000, 20000)) for _ in range(2000)]
    log.info("Took %.2f seconds to generate %s kb of random data", time.time()-start, sum(len(data) for data in datas) / 1000)

    ########################
    ###   SENDING
    ########################

    log.info("Sending data to sqs")
    start = time.time()
    write_pool = SingleItemPool(100, sqs_write, sqs_queue, s3_bucket)
    write_pool.process(datas)
    log.info("sending %s things took %.2f seconds", len(datas), time.time() - start)

    ########################
    ###   DELETION
    ########################

    # Setup pools to delete messages when we are finished with them

    def s3_delete(keys, bucket):
        """Processor to delete s3 keys"""
        counts['s3_deleted_count'] += len(keys)
        log.info("Deleting %d s3 keys", len(keys))
        bucket.delete_keys(keys)
    s3_deleter = BulkItemsPool(100, 10, s3_delete, s3_bucket)

    def sqs_delete(item):
        """Processor to delete sqs items"""
        counts['sqs_deleted_count'] += 1
        log.info("Deleting sqs item")
        item.delete()
    sqs_deleter = SingleItemPool(500, sqs_delete)

    ########################
    ###   RETRIEVAL
    ########################

    log.info("Reading data from sqs")
    times = dict(end = 0)
    start = time.time()

    def retrieve(retrieved):
        """Deal with something we got from the sqs queue"""
        if retrieved.successful:
            if 'message' in retrieved.payload:
                decoded = base64.b64decode(retrieved.payload['message'])
                log.info("retrieved %d", len(decoded))
            else:
                log.warn("retreived without message")

        if retrieved.successful or retrieved.discard:
            # Putting sqs result ans s3 keys into queues for deletion
            sqs_deleter.add_handler(retrieved.sqs_result)
            if retrieved.s3_key:
                s3_deleter.add_handler(retrieved.s3_key)

        # Record when we processed the last thing
        times['end'] = time.time()
        counts['processed'] += 1

    def reader(queue, bucket, dealer):
        """Read from sqs and pass onto our dealer"""
        results = list(sqs_read(queue, bucket))
        counts['received'] += len(results)
        for result in results:
            dealer.add_handler(result)

    # Pools for reading data and dealing with that data
    deal_pool = SingleItemPool(500, retrieve)
    read_pool = SingleItemPool(500, reader, sqs_queue, s3_bucket, deal_pool)

    # Naive attempt at determining when we have stopped
    same = False
    last_received = counts['received']
    while True:
        read_pool.add_handlers(20)

        time.sleep(0.5)
        now_same = counts['received'] == last_received
        if same and now_same:
            read_pool.join()
            deal_pool.join()
            break
        elif now_same:
            same = True
            time.sleep(2)
            continue
        else:
            same = False

        last_received = counts['received']

    ########################
    ###   CLEANUP
    ########################

    log.info("Getting %d things took %.2f seconds", counts['processed'], times['end'] - start)

    log.info("Making sure the sqs and s3 things are deleted")
    s3_deleter.join()
    sqs_deleter.join()

    log.info("Deleted %d s3 keys, %d sqs items", counts['s3_deleted_count'], counts['sqs_deleted_count'])
