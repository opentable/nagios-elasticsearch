#!/usr/bin/python3
import nagiosplugin
import urllib.request
import argparse

try:
    import json
except ImportError:
    import simplejson as json


class ESNodesResource(nagiosplugin.Resource):
    def __init__(self, host, port, expected_nodes):
        self.host = host
        self.port = port
        self.expected_nodes = expected_nodes

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

        active_cluster_nodes = es_cluster_health['number_of_nodes']
        return [nagiosplugin.Metric('active_nodes', active_cluster_nodes, min=0, context='nodes')]


class ESNodesContext(nagiosplugin.Context):
    def __init__(self, name, expected_nodes):
        super().__init__(name)
        self.expected_nodes = expected_nodes
        
    def evaluate(self, metric, resource):
        if metric.value < self.expected_nodes:
            return nagiosplugin.Result(
                nagiosplugin.Critical,
                f"Number of nodes in the cluster is reporting as '{metric.value}' but we expected '{self.expected_nodes}'"
            )
        return nagiosplugin.Result(
            nagiosplugin.Ok,
            f"Number of nodes in the cluster is '{metric.value}' which is >= {self.expected_nodes} as expected"
        )


@nagiosplugin.guarded
def main():
    argp = argparse.ArgumentParser(description='Check Elasticsearch node count')
    argp.add_argument('-E', '--expected-nodes-in-cluster', required=True, type=int,
                     help='This is the expected number of nodes in the cluster')
    argp.add_argument('-H', '--host', required=True, help='The cluster to check')
    argp.add_argument('-P', '--port', default=9200, type=int, help='The ES port - defaults to 9200')
    
    args = argp.parse_args()
    
    check = nagiosplugin.Check(
        ESNodesResource(args.host, args.port, args.expected_nodes_in_cluster),
        ESNodesContext('nodes', args.expected_nodes_in_cluster)
    )
    
    check.main()


if __name__ == "__main__":
    main()
