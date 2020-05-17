"""
   panoptes: Service to synchronise storage.
"""

import json
import logging
import os
import requests
import traceback
import time

try:
    from queue import Queue
except ImportError:
    import Queue

from threading import Thread

try:
    from urlparse import urljoin, urlparse
except:
    from urllib.parse import urljoin, urlparse

from sseclient import SSEClient
from rucio.client import Client

_LOGGER = logging.getLogger(__name__)


def submit_transfer_to_fts(source_url, bytes, adler32, destination_url, proxy, fts_host):
    transfer_request = {'files': [{
        'sources': [source_url],
        'destinations': [destination_url],
        'filesize': bytes,
        'checksum': 'adler32:%s' % adler32}],
        'params': {'verify_checksum': True}}

    # response = session.get('%s/api-docs/schema/submit' % fts_host)
    # schema = response.json()
    # from jsonschema import validate
    # print (validate(instance=transfer_request, schema=schema))

    response = requests.post(
        '%s/jobs' % fts_host,
        json=transfer_request,
        cert=proxy,
        headers={'Content-Type': 'application/json'})
    # if response.status_code == 200:
    _LOGGER.info("Transfer from {} to {} has been submitted to FTS ({})".format(source_url, destination_url, response.content))


def submit_transfer_to_rucio(name, source_url, bytes, adler32):
    _LOGGER.info("Here")
    # transfer pre-prod -> prod -> snic
    rucio_client = Client()

    # TODO: scope should be extracted from the path: Top directory
    scope = 'functional_tests'

    try:
        replica = {
            'scope': scope,
            'name': name,
            'pfn': source_url,
            'bytes': int(bytes),
            'adler32': adler32}

        _LOGGER.debug('Register replica {}'.format(str(replica)))

        rse = 'NDGF-PREPROD'
        account = 'garvin'

        rucio_client.add_replicas(
            rse=rse,
            files=[replica])

        kwargss = [
            {'rse_expression': 'NDGF-PREPROD', 'lifetime': 86400},
            {'rse_expression': 'NDGF', 'source_replica_expression': 'NDGF-PREPROD',
            'lifetime': 86400},
            {'rse_expression': 'SNIC', 'source_replica_expression': 'NDGF',
            'lifetime': 86400}]

        for kwargs in kwargss:
            rule = rucio_client.add_replication_rule(
                dids=[{'scope': scope, 'name': name}],
                account=account,
                copies=1,
                grouping='NONE',
                weight=None,
                locked=False,
                **kwargs)
            _LOGGER.info('Added rule for file to {}: {}'.format(kwargs, rule))
    except:
        _LOGGER.error(traceback.format_exc())


def register_in_rucio(source_url, bytes, adler32):
    _LOGGER.info("Here")
    # transfer pre-prod -> prod -> snic
    rucio_client = Client()

    # TODO: scope should be extracted from the path: Top directory
    scope = 'user.pmillar'

    name = os.path.basename(urlparse(source_url).path)

    try:
        replica = {
            'scope': scope,
            'name': name,
            'pfn': source_url,
            'bytes': int(bytes),
            'adler32': adler32}

        _LOGGER.debug('Register replica {}'.format(str(replica)))

        rse = 'XDCDESY_PAULM_1_TEST'

        rucio_client.add_replicas(
            rse=rse,
            files=[replica])
    except:
        _LOGGER.error(traceback.format_exc())


def action_fts_copy(new_files, session, fts_host):
    print("Action: FTS-COPY starting", flush=True)
    while True:
        try:
            source_url, destination_url = new_files.get()
            # Workaround: slight risk the client receives the `IN_CLOSE_WRITE`
            # event before the upload is completed. TBR.
            for _ in range(10):
                # Get this info with dav
                # Can use the namespace operation later
                response = session.head(source_url, headers={'Want-Digest': 'adler32'})
                if response.status_code == 200:
                    break
                time.sleep(0.1)
            _LOGGER.debug(response.headers)

            adler32 = response.headers['Digest'].replace('adler32=', '')
            bytes = int(response.headers['Content-Length'])

            submit_transfer_to_fts(
                source_url=source_url,
                bytes=bytes,
                adler32=adler32,
                destination_url=destination_url,
                proxy=session.cert,
                fts_host=fts_host)
        except:
            _LOGGER.error(traceback.format_exc())
        finally:
            new_files.task_done()


def action_rucio_copy(new_files, session):
    print("Action: RUCIO-COPY starting", flush=True)
    while True:
        try:
            source_url, destination_url = new_files.get()
            # Workaround: slight risk the client receives the `IN_CLOSE_WRITE`
            # event before the upload is completed. TBR.
            for _ in range(10):
                # Get this info with dav
                # Can use the namespace operation later
                response = session.head(source_url, headers={'Want-Digest': 'adler32'})
                if response.status_code == 200:
                    break
                time.sleep(0.1)
            _LOGGER.debug(response.headers)

            adler32 = response.headers['Digest'].replace('adler32=', '')
            bytes = int(response.headers['Content-Length'])

            submit_transfer_to_rucio(
                name=name,
                source_url=source_url,
                bytes=bytes,
                adler32=adler32
            )
        except:
            _LOGGER.error(traceback.format_exc())
        finally:
            new_files.task_done()


def action_rucio_register(new_files, session):
    print("Action: RUCIO-REGISTER starting", flush=True)
    s = requests.Session()
    s.verify = '/etc/grid-security/certificates'
    while True:
        try:
            source_url, destination_url = new_files.get()
            print ("Registering: " + source_url, flush=True)
            # Workaround: slight risk the client receives the `IN_CLOSE_WRITE`
            # event before the upload is completed. TBR.
            for _ in range(10):
                # Get this info with dav
                # Can use the namespace operation later
                response = s.head(source_url, headers={'Want-Digest': 'adler32'})
                if response.status_code == 200:
                    break
                time.sleep(0.1)
            _LOGGER.debug(response.headers)

            adler32 = response.headers['Digest'].replace('adler32=', '')
            bytes = int(response.headers['Content-Length'])

            register_in_rucio(
                source_url=source_url,
                bytes=bytes,
                adler32=adler32)
        except:
            _LOGGER.error(traceback.format_exc())
        finally:
            new_files.task_done()

def action_print(new_files):
    print("Action: PRINT starting", flush=True)
    while True:
        try:
            source_url, destination_url = new_files.get()
            print (source_url + " -> " + destination_url, flush=True)
        except:
            _LOGGER.error(traceback.format_exc())
        finally:
            new_files.task_done()


def build_thread_for_action(**attributes):
    session = attributes['client'].session
    action = attributes['action']
    new_files = attributes['new_files']
    if action == "fts-copy":
        fts_host = attributes['fts_host']
        return Thread(target=action_fts_copy, args=(new_files, session, fts_host))
    elif action == "rucio-copy":
        return Thread(target=action_rucio_copy, args=(new_files, session))
    elif action == "rucio-register":
        return Thread(target=action_rucio_register, args=(new_files, session))
    elif action == "print":
        return Thread(target=action_print, args=(new_files,))
    else:
        raise ValueError("Unknown action: {}".format(action))


def main(root_path, source, destination, recursive, **attributes):
    '''
    main function
    '''
    new_files = Queue(maxsize=0)
    attributes['new_files'] = new_files
    worker = build_thread_for_action(**attributes)
    worker.setDaemon(True)
    worker.start()

    base_path = urlparse(source).path
    paths = [os.path.normpath(root_path + '/' + base_path)]
    client = attributes['client']
    if recursive:
        directories = [urlparse(source).path]
        _LOGGER.debug("Scan {}".format(base_path))
        while directories:
            prefix = directories.pop()
            response = client.namespace.get_file_attributes(path=prefix, children=True)
            for entry in response["children"]:
                if entry["fileType"] == "DIR":
                    directory = os.path.normpath(prefix + '/' + entry["fileName"])
                    _LOGGER.debug("Directory found {}".format(directory))
                    directories.append(directory)
                    paths.append(os.path.normpath(root_path + '/' + directory))

    watches = {}
    while True:
        response = client.events.register()
        channel = response.headers['Location']
        _LOGGER.info("Channel is {}".format(channel))

        id = channel[channel.find('/api/v1/events/channels/') + 24:]

        for path in paths:
            response = client.events.subscribe(type='inotify', id=id, body={"path": path})
            watch = response.headers['Location']
            _LOGGER.debug("Watch on {} is {}".format(path, watch))
            watches[watch] = path

        messages = SSEClient(channel, session=client.session)
        try:
            for msg in messages:
                _LOGGER.debug("Event {}:".format(msg.id))
                _LOGGER.debug("    event: {}".format(msg.event))
                _LOGGER.debug("    data: {}".format(msg.data))
                data = json.loads(msg.data)
                if 'event' in data and data['event']['mask'] == ['IN_CLOSE_WRITE']:
                    name = data['event']['name']
                    full_path = watches[data["subscription"]]
                    short_path = os.path.relpath(full_path, root_path)[len(base_path) - 1:]
                    source_url = urljoin(source, os.path.normpath(os.path.join(short_path, name)))
                    _LOGGER.info('New file detected: ' + source_url)
                    destination_url = urljoin(destination, os.path.normpath(source_url[len(source):]))
                    _LOGGER.info('Request to copy it to: ' + destination_url)

                    new_files.put((source_url, destination_url))
                elif 'event' in data and data['event']['mask'] == ["IN_CREATE", "IN_ISDIR"]:
                    name = data['event']['name']
                    full_path = watches[data["subscription"]]
                    dir_path = os.path.normpath(full_path + '/' + name)
                    _LOGGER.info('New directory detected: ' + dir_path)
                    response = client.events.subscribe(type='inotify', id=id, body={"path": dir_path})
                    watch = response.headers['Location']
                    _LOGGER.debug("Watch on {} is {}".format(dir_path, watch))
                    watches[watch] = dir_path
                    paths.append(dir_path)

        except requests.exceptions.HTTPError as exc:
            _LOGGER.error(str(exc))
#           raise
            _LOGGER.info('Re-register and Re-subscribe to channel')
