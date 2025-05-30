#!/usr/bin/python3
import nagiosplugin
import urllib.request
import argparse

try:
    import json
except ImportError:
    import simplejson as json


class ESSplitBrainResource(nagiosplugin.Resource):
    def __init__(self, nodes, port):
        self.nodes = nodes
        self.port = port
        self.cluster_name = None

    def probe(self):
        nodes = self.nodes.split(",")
        port = self.port
        masters = []
        responding_nodes = []
        failed_nodes = []

        for node in nodes:
            try:
                response = urllib.request.urlopen(
                        f'http://{node}:{port}/_cluster/state/nodes,master_node/')
                response_body = response.read().decode('utf-8')
                response = json.loads(response_body)
            except (urllib.error.HTTPError, urllib.error.URLError) as e:
                failed_nodes.append(f"{node} - {getattr(e, 'reason', str(e))}")
                continue

            if isinstance(response, dict):
                self.cluster_name = str(response['cluster_name'])
                master = str(
                        response['nodes'][response['master_node']]['name']
                        )
                responding_nodes.append(node)
                if master not in masters:
                    masters.append(master)

        return [
            nagiosplugin.Metric('masters', masters),
            nagiosplugin.Metric('responding_nodes', len(responding_nodes)),
            nagiosplugin.Metric('total_nodes', len(nodes)),
            nagiosplugin.Metric('failed_nodes', failed_nodes)
        ]


class ESSplitBrainContext(nagiosplugin.Context):
    def evaluate(self, metric, resource):
        if metric.name == 'masters':
            masters = metric.value
            if len(resource.resource.probe()[1].value) == 0:  # responding_nodes
                return nagiosplugin.Result(
                    nagiosplugin.Unknown,
                    hint=f"All cluster nodes unresponsive:\n{chr(10).join(resource.resource.probe()[3].value)}"  # failed_nodes
                )
            elif len(masters) != 1:
                return nagiosplugin.Result(
                    nagiosplugin.Critical,
                    hint=f"{len(masters)} masters ({', '.join(masters)}) found in {resource.resource.cluster_name} cluster"
                )
            else:
                failed_nodes = resource.resource.probe()[3].value
                responding_nodes = resource.resource.probe()[1].value
                total_nodes = resource.resource.probe()[2].value
                
                if len(failed_nodes) == 0:
                    return nagiosplugin.Result(
                        nagiosplugin.Ok,
                        hint=f"{responding_nodes}/{total_nodes} nodes have same master"
                    )
                else:
                    return nagiosplugin.Result(
                        nagiosplugin.Ok,
                        hint=f"{responding_nodes}/{total_nodes} nodes have same master\n"
                             f"{len(failed_nodes)} unresponsive nodes:\n{chr(10).join(failed_nodes)}"
                    )
        return nagiosplugin.Result(nagiosplugin.Ok)


@nagiosplugin.guarded
def main():
    argp = argparse.ArgumentParser(description='Check Elasticsearch for split-brain situation')
    argp.add_argument('-N', '--nodes', required=True, help='Cluster nodes (comma-separated)')
    argp.add_argument('-P', '--port', default=9200, type=int, help='The ES port - defaults to 9200')
    
    args = argp.parse_args()
    
    check = nagiosplugin.Check(
        ESSplitBrainResource(args.nodes, args.port),
        ESSplitBrainContext('masters')
    )
    
    check.main()


if __name__ == "__main__":
    main()
