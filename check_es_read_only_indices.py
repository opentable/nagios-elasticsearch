#!/usr/bin/python3
import argparse
import json
import socket
import urllib.error
import urllib.request

import nagiosplugin


READ_ONLY_SETTING = 'index.blocks.read_only_allow_delete'


def count_read_only_indices(payload):
    if not isinstance(payload, dict):
        raise ValueError('expected a JSON object')

    read_only_count = 0
    for index_name, index_data in payload.items():
        if not isinstance(index_data, dict):
            raise ValueError(f'invalid settings for index {index_name}')

        settings = index_data.get('settings') or {}
        defaults = index_data.get('defaults') or {}
        if not isinstance(settings, dict) or not isinstance(defaults, dict):
            raise ValueError(f'invalid settings for index {index_name}')

        value = settings.get(
            READ_ONLY_SETTING,
            defaults.get(READ_ONLY_SETTING, 'false'),
        )
        if value is True or str(value).lower() == 'true':
            read_only_count += 1

    return read_only_count, len(payload)


class ESReadOnlyIndices(nagiosplugin.Resource):
    def __init__(self, host, port, timeout):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.total_count = 0

    def probe(self):
        url = (
            f'http://{self.host}:{self.port}/_all/_settings/'
            f'{READ_ONLY_SETTING}?flat_settings=true&include_defaults=true'
        )

        try:
            response = urllib.request.urlopen(url, timeout=self.timeout)
            response_body = response.read().decode('utf-8')
        except urllib.error.HTTPError as error:
            raise nagiosplugin.CheckError(
                f'Elasticsearch API returned HTTP {error.code} for {self.host}'
            )
        except (urllib.error.URLError, socket.timeout, OSError) as error:
            raise nagiosplugin.CheckError(
                f'Cannot query {self.host}: {error}'
            )

        try:
            payload = json.loads(response_body)
            read_only_count, self.total_count = count_read_only_indices(payload)
        except (ValueError, TypeError) as error:
            raise nagiosplugin.CheckError(
                f'Invalid Elasticsearch response from {self.host}: {error}'
            )

        return [
            nagiosplugin.Metric(
                'read_only_indices',
                read_only_count,
                min=0,
                context='read_only_indices',
            ),
        ]


class ESReadOnlyIndicesContext(nagiosplugin.Context):
    def evaluate(self, metric, resource):
        es = resource
        message = (
            f'{metric.value} of {es.total_count} Elasticsearch indices '
            f'are read-only on {es.host}'
        )
        if metric.value > 0:
            return nagiosplugin.Result(nagiosplugin.Critical, hint=message)
        return nagiosplugin.Result(nagiosplugin.Ok, hint=message)


@nagiosplugin.guarded
def main():
    argp = argparse.ArgumentParser(
        description='Check Elasticsearch indices for read-only protection'
    )
    argp.add_argument('-H', '--host', required=True,
                      help='The cluster to check')
    argp.add_argument('-P', '--port', default=9200, type=int,
                      help='The ES port - defaults to 9200')
    argp.add_argument('-T', '--timeout', default=10, type=int,
                      help='HTTP timeout in seconds - defaults to 10')

    args = argp.parse_args()

    check = nagiosplugin.Check(
        ESReadOnlyIndices(args.host, args.port, args.timeout),
        ESReadOnlyIndicesContext('read_only_indices'),
    )
    check.main()


if __name__ == '__main__':
    main()
