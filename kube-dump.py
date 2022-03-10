#!/usr/bin/env python3

import os
import json
import shutil
import logging
import argparse

import yaml
import kubernetes.client
import kubernetes.client.rest
import kubernetes.config

from kubernetes.client import models

log = logging.getLogger(__name__)


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('output_dir', help='directory where to dump')
    arg_parser.add_argument('--in-cluster', action='store_true', help='configure with in cluster kubeconfig')
    arg_parser.add_argument('--no-clean', action='store_true', help='don\'t clean output directory on start')
    arg_parser.add_argument('--no-skip-owned', action='store_true', help='don\'t skip objects with ownerReferences')
    arg_parser.add_argument('--fast', action='store_true', help='don\'t load original YAML from server')
    arg_parser.add_argument('--format', choices=['json', 'yaml'], default='yaml')
    arg_parser.add_argument('--skip-gk', nargs='+', default=[], help='skip this Group/Kind')
    arg_parser.add_argument('--log-level', default='WARNING')
    args = arg_parser.parse_args()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=args.log_level)

    if args.in_cluster:
        kubernetes.config.load_incluster_config()
    else:
        kubernetes.client.configuration.host = 'http://127.0.0.1:8001'

    dumper = Dumper(args.output_dir)
    dumper.format = args.format
    dumper.clean_output = not args.no_clean
    dumper.skip_owned = not args.no_skip_owned
    dumper.fast = not args.no_skip_owned
    dumper.improved_yaml = not args.fast
    dumper.skip_gks = args.skip_gk
    dumper.dump_all()


class Dumper:
    def __init__(self, output_dir):
        self.client = kubernetes.client.ApiClient()
        self.output_dir = output_dir
        self.format = 'yaml'
        self.clean_output = True
        self.skip_owned = True
        self.improved_yaml = True
        self.skip_gks = []

    def call(self, *args, **kwargs):
        kwargs.setdefault('response_type', object)
        kwargs.setdefault('auth_settings', ['BearerToken'])
        kwargs.setdefault('_return_http_data_only', True)
        return self.client.call_api(*args, **kwargs)

    def dump_all(self):
        if self.clean_output:
            shutil.rmtree(self.output_dir, ignore_errors=True)

        api_groups = self.call('/apis/', 'GET', response_type='V1APIGroupList').groups

        # remove deprecated group
        api_groups = [g for g in api_groups if g.preferred_version.group_version != 'extensions/v1beta1']

        # add core "v1" API group
        core_api_group_version = models.V1GroupVersionForDiscovery(group_version='v1', version='v1')
        core_api_group = models.V1APIGroup(
            api_version='v1',
            kind='APIGroup',
            preferred_version=core_api_group_version,
            versions=[core_api_group_version])
        api_groups = [core_api_group] + api_groups

        for g in api_groups:
            for v in g.versions:
                if g.preferred_version.group_version == v.group_version:
                    g.preferred_version = v
                resource_path = get_api_group_version_resource_path(v.group_version)
                v.resources = self.call(resource_path, 'GET', response_type='V1APIResourceList').resources
                v.resources = [r for r in v.resources if '/' not in r.name]
                v.resources = [r for r in v.resources if 'list' in r.verbs]
                for r in v.resources:
                    r.g = g
                    r.gv = v

        # first we assume that resources of latest version are best
        for g in api_groups:
            g.best_resources = {}
            for v in g.versions:
                for r in v.resources:
                    g.best_resources[r.kind] = r

        # if there is preferred version of resource, use it
        for g in api_groups:
            for r in g.preferred_version.resources:
                g.best_resources[r.kind] = r

        for g in api_groups:
            for resource in g.best_resources.values():
                if resource.g is core_api_group:
                    api_group = '_core'
                else:
                    api_group = resource.gv.group_version.split('/', maxsplit=1)[0]

                gk = f'{api_group}/{resource.kind}'
                if gk in self.skip_gks:
                    continue

                resource_path = get_api_group_version_resource_path(resource.gv.group_version)
                self.dump_resource(api_group, resource_path, resource)

    def dump_resource(self, api_group, resource_path, resource):
        list_path = '{}/{}'.format(resource_path, resource.name)
        result = self.call(list_path, 'GET')
        objects = result['items']

        for obj in objects:
            obj_name = obj['metadata']['name']

            if resource.namespaced:
                obj_namespace = obj['metadata']['namespace']
            else:
                obj_namespace = '_'

            obj['apiVersion'] = result['apiVersion']
            obj['kind'] = resource.kind

            log.info('%s/%s %s/%s', api_group, resource.kind, obj_namespace, obj_name)

            if self.skip_owned and 'ownerReferences' in obj['metadata']:
                continue

            resource_dir = os.path.join(self.output_dir, obj_namespace, api_group, resource.kind)
            os.makedirs(resource_dir, exist_ok=True)

            object_filepath = os.path.join(resource_dir, obj_name)

            if self.format == 'yaml':
                with open(object_filepath + '.yaml', 'w') as fp:
                    if self.improved_yaml:
                        if resource.namespaced:
                            obj_path = '{}/namespaces/{}/{}/{}'.format(resource_path, obj_namespace, resource.name, obj_name)
                        else:
                            obj_path = '{}/{}'.format(list_path, obj_name)

                        try:
                            data = self.call(obj_path, 'GET', header_params={
                                'Accept': 'application/yaml',
                            })
                            fp.write(data)
                        except kubernetes.client.rest.ApiException as e:
                            if e.status == 406:
                                log.warning('Can\'t get improved yaml %s, fallback to ordinary yaml', obj_path)
                                yaml.safe_dump(obj, default_flow_style=False, stream=fp)
                            else:
                                raise e
                    else:
                        yaml.safe_dump(obj, default_flow_style=False, stream=fp)
            elif self.format == 'json':
                with open(object_filepath + '.json', 'w') as fp:
                    json.dump(obj, fp, indent=2)


def get_api_group_version_resource_path(api_group_version):
    if api_group_version == 'v1':
        return '/api/v1'
    else:
        return '/apis/' + api_group_version


if __name__ == '__main__':
    main()
