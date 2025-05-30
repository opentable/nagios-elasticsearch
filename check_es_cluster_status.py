#!/usr/bin/python3
import nagiosplugin
import urllib.request
import argparse
import sys

try:
    import json
except ImportError:
    import simplejson as json


class ESClusterHealth(nagiosplugin.Resource):
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def probe(self):
        try:
            response = urllib.request.urlopen(f'http://{self.host}:{self.port}/_cluster/health')
        except urllib.error.HTTPError as e:
            raise nagiosplugin.CheckError(f"API failure:\n\n{str(e)}")
        except urllib.error.URLError as e:
            raise nagiosplugin.CheckError(f"Connection error: {e.reason}")

        response_body = response.read().decode('utf-8')

        try:
            es_cluster_health = json.loads(response_body)
        except ValueError:
            raise nagiosplugin.CheckError("API returned nonsense")

        return [nagiosplugin.Metric('status', es_cluster_health['status'].lower(), context='status')]


class ESClusterStatusContext(nagiosplugin.Context):
    def evaluate(self, metric, resource):
        status = metric.value
        if status == 'red':
            return nagiosplugin.Result(nagiosplugin.Critical, "Cluster status is currently reporting as Red")
        elif status == 'yellow':
            return nagiosplugin.Result(nagiosplugin.Warn, "Cluster status is currently reporting as Yellow")
        else:
            return nagiosplugin.Result(nagiosplugin.Ok, "Cluster status is currently reporting as Green")


@nagiosplugin.guarded
def main():
    argp = argparse.ArgumentParser(description='Check Elasticsearch cluster health')
    argp.add_argument('-H', '--host', required=True, help='The cluster to check')
    argp.add_argument('-P', '--port', default=9200, type=int, help='The ES port - defaults to 9200')
    
    args = argp.parse_args()
    
    check = nagiosplugin.Check(
        ESClusterHealth(args.host, args.port),
        ESClusterStatusContext('status')
    )
    check.main()


if __name__ == "__main__":
    main()
