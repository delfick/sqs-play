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
from Queue import Queue, Empty
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

def queue_execute(data_queue, func, *args):
    """Keep reading from a queue and execute func with each thing that comes through"""
    while True:
        try:
            data = data_queue.get(block=False)
        except Empty:
            return

        func(data, *args)

class NoKey(Exception):
    """For when an sqs message references an s3 key that don't exist"""
    pass

class BadKey(Exception):
    """For when we can't read an s3 key for some non 404 reason"""
    pass

########################
###   SQS
########################

def sqs_write(data, sqs_queue, bucket):
    """
        Write a message to an sqs queue
        If the message is too big for the queue, then write to an s3 bucket instead
    """
    m = RawMessage()
    message = {'what':3, 'message':base64.b64encode(data)}
    body = json.dumps(message)
    if len(body) >= 65536:
        key = s3_write(bucket, body)
        body = json.dumps({'s3':key})
        log.info('Adding s3 %s', key)
    else:
        log.info('Adding to sqs')
    m.set_body(body)
    sqs_queue.write(m)

def sqs_read(queue, bucket):
    """
        Read a message from sqs and json.loads the body
        If the body is a pointer to s3, then get from there and json.loads that
    """
    results = queue.get_messages(num_messages=10)
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

def sqs_poll(queue, bucket, into):
    """Poll from sqs and put the results into the provided queue"""
    while True:
        for result in sqs_read(queue, bucket):
            into.put(result)
        else:
            time.sleep(2)

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
    data_queue = Queue()
    send_threads = [gevent.spawn(queue_execute, data_queue, sqs_write, sqs_queue, s3_bucket) for _ in range(500)]
    for data in datas:
        data_queue.put(data)
    gevent.joinall(send_threads)
    log.info("sending %s things took %.2f seconds", len(datas), time.time() - start)

    ########################
    ###   DELETION
    ########################

    # Setup threads to delete messages when we are finished with them
    s3_delete_queue = Queue()
    sqs_delete_queue = Queue()
    info = {'finished' : False}

    def s3_deleter(queue, bucket, group_count=20):
        """Try to group up deleting s3 keys"""
        buf = []
        while True:
            try:
                buf.append(queue.get(timeout=1))
                if len(buf) > group_count:
                    bucket.delete_keys(buf)
                    buf = []
            except Empty:
                if info['finished']:
                    break

        # Make sure to remove the leftovers
        if buf:
            bucket.delete_keys(buf)

    def sqs_deleter(queue):
        """Delete each sqs result as it comes in"""
        while True:
            try:
                queue.get(timeout=1).delete()
            except Empty:
                if info['finished']:
                    break

    # Many to delete sqs seeing as that has to happen one at a time
    # Only one for s3, as that gets bulked up
    delete_threads = [gevent.spawn(sqs_deleter, sqs_delete_queue) for _ in range(500)]
    gevent.spawn(s3_deleter, s3_delete_queue, s3_bucket)
    time.sleep(0.01)

    ########################
    ###   RETRIEVAL
    ########################

    log.info("Reading data from sqs")
    end = 0
    start = time.time()
    into = Queue()
    read_threads = [gevent.spawn(sqs_poll, sqs_queue, s3_bucket, into) for i in range(500)]
    time.sleep(0.01)

    # Receive everything from the Queue we created
    count = 0
    while True:
        try:
            end = time.time()
            retrieved = into.get(timeout=5)
        except Empty:
            break

        if retrieved.successful:
            if 'message' in retrieved.payload:
                decoded = base64.b64decode(retrieved.payload['message'])
                log.info("retrieved %d", len(decoded))
            else:
                log.warn("retreived without message")

        if retrieved.successful or retrieved.discard:
            # Putting sqs result ans s3 keys into queues for deletion
            sqs_delete_queue.put(retrieved.sqs_result)
            if retrieved.s3_key:
                s3_delete_queue.put(retrieved.s3_key)
            count += 1

    ########################
    ###   CLEANUP
    ########################

    log.info("Getting %d things took %.2f seconds", count, end - start)
    gevent.killall(read_threads)

    log.info("Making sure the sqs and s3 things are deleted")
    info['finished'] = True
    gevent.joinall(delete_threads)
