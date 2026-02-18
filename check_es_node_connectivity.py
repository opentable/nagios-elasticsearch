#!/usr/bin/python3
import nagiosplugin
import urllib.request
import argparse

try:
    import json
except ImportError:
    import simplejson as json


class ESNodeConnectivity(nagiosplugin.Resource):
    def __init__(self, nodes, port, timeout):
        self.nodes = nodes
        self.port = port
        self.timeout = timeout
        self.failed_nodes = []
        self.total_count = 0

    def probe(self):
        nodes = self.nodes.split(",")
        self.total_count = len(nodes)
        self.failed_nodes = []

        for node in nodes:
            try:
                urllib.request.urlopen(
                    f'http://{node}:{self.port}/_cluster/health',
                    timeout=self.timeout
                )
            except (urllib.error.HTTPError, urllib.error.URLError) as e:
                self.failed_nodes.append(f"{node} ({getattr(e, 'reason', str(e))})")
            except Exception as e:
                self.failed_nodes.append(f"{node} ({str(e)})")

        return [
            nagiosplugin.Metric('connectivity', len(self.failed_nodes), context='connectivity'),
        ]


class ESNodeConnectivityContext(nagiosplugin.Context):
    def evaluate(self, metric, resource):
        es = resource.resource
        failed = es.failed_nodes
        total = es.total_count

        if len(failed) == 0:
            return nagiosplugin.Result(
                nagiosplugin.Ok,
                hint=f"All {total} nodes reachable"
            )

        return nagiosplugin.Result(
            nagiosplugin.Critical,
            hint=f"Cluster API call timed out for {len(failed)}/{total} nodes:\n" +
                 "\n".join(failed) +
                 "\nCluster may be degraded - avoid node restarts"
        )


@nagiosplugin.guarded
def main():
    argp = argparse.ArgumentParser(
        description='Check connectivity to individual Elasticsearch nodes'
    )
    argp.add_argument('-N', '--nodes', required=True,
                      help='Cluster nodes (comma-separated)')
    argp.add_argument('-P', '--port', default=9200, type=int,
                      help='The ES port - defaults to 9200')
    argp.add_argument('-T', '--timeout', default=5, type=int,
                      help='Per-node HTTP timeout in seconds - defaults to 5')

    args = argp.parse_args()

    check = nagiosplugin.Check(
        ESNodeConnectivity(args.nodes, args.port, args.timeout),
        ESNodeConnectivityContext('connectivity')
    )
    check.main()


if __name__ == "__main__":
    main()
