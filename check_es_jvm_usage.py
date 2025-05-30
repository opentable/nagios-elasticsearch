#!/usr/bin/python3
import nagiosplugin
import urllib.request
import argparse
import sys

try:
    import json
except ImportError:
    import simplejson as json


class ESJVMHealth(nagiosplugin.Resource):
    def __init__(self, host, port, warning, critical):
        self.host = host
        self.port = port
        self.warning = warning
        self.critical = critical

    def probe(self):
        try:
            response = urllib.request.urlopen(f'http://{self.host}:{self.port}/_nodes/stats/jvm')
        except urllib.error.HTTPError as e:
            raise nagiosplugin.CheckError(f"API failure:\n\n{str(e)}")
        except urllib.error.URLError as e:
            raise nagiosplugin.CheckError(f"Connection error: {e.reason}")

        response_body = response.read().decode('utf-8')

        try:
            nodes_jvm_data = json.loads(response_body)
        except ValueError:
            raise nagiosplugin.CheckError("API returned nonsense")

        criticals = 0
        critical_details = []
        warnings = 0
        warning_details = []

        nodes = nodes_jvm_data['nodes']
        for node in nodes:
            jvm_percentage = nodes[node]['jvm']['mem']['heap_used_percent']
            node_name = nodes[node]['host']
            if int(jvm_percentage) >= self.critical:
                criticals = criticals + 1
                critical_details.append(f"{node_name} currently running at {jvm_percentage}% JVM mem")
            elif (int(jvm_percentage) >= self.warning and
                  int(jvm_percentage) < self.critical):
                warnings = warnings + 1
                warning_details.append(f"{node_name} currently running at {jvm_percentage}% JVM mem")

        return [
            nagiosplugin.Metric('jvm_critical_nodes', criticals, min=0),
            nagiosplugin.Metric('jvm_warning_nodes', warnings, min=0),
            nagiosplugin.Metric('critical_details', '\n'.join(critical_details) if critical_details else ''),
            nagiosplugin.Metric('warning_details', '\n'.join(warning_details) if warning_details else '')
        ]


class ESJVMSummary(nagiosplugin.Summary):
    def ok(self, results):
        return "All nodes in the cluster are currently below the % JVM mem warning threshold"

    def problem(self, results):
        if results['jvm_critical_nodes'].metric.value > 0:
            return (f"There are '{results['jvm_critical_nodes'].metric.value}' node(s) in the cluster that have "
                   f"breached the % JVM heap usage critical threshold of {args.critical_threshold}%. They are:\n"
                   f"{results['critical_details'].metric.value}")
        elif results['jvm_warning_nodes'].metric.value > 0:
            return (f"There are '{results['jvm_warning_nodes'].metric.value}' node(s) in the cluster that have "
                   f"breached the % JVM mem usage warning threshold of {args.warning_threshold}%. They are:\n"
                   f"{results['warning_details'].metric.value}")


class ESJVMContext(nagiosplugin.Context):
    def evaluate(self, metric, resource):
        if metric.name == 'jvm_critical_nodes' and metric.value > 0:
            return nagiosplugin.Result(nagiosplugin.Critical)
        elif metric.name == 'jvm_warning_nodes' and metric.value > 0:
            return nagiosplugin.Result(nagiosplugin.Warn)
        return nagiosplugin.Result(nagiosplugin.Ok)


@nagiosplugin.guarded
def main():
    global args
    argp = argparse.ArgumentParser(description='Check Elasticsearch JVM usage')
    argp.add_argument('-H', '--host', required=True, help='The cluster to check')
    argp.add_argument('-P', '--port', default=9200, type=int, help='The ES port - defaults to 9200')
    argp.add_argument('-C', '--critical-threshold', default=97, type=int, 
                     help='The level at which we throw a CRITICAL alert - defaults to 97% of the JVM setting')
    argp.add_argument('-W', '--warning-threshold', default=90, type=int,
                     help='The level at which we throw a WARNING alert - defaults to 90% of the JVM setting')
    
    args = argp.parse_args()
    
    check = nagiosplugin.Check(
        ESJVMHealth(args.host, args.port, args.warning_threshold, args.critical_threshold),
        ESJVMContext('jvm'),
        ESJVMContext('jvm_critical_nodes'),
        ESJVMContext('jvm_warning_nodes'),
        ESJVMSummary()
    )
    check.main()


if __name__ == "__main__":
    main()
