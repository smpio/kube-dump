import os
import json
import shutil
import logging
import argparse

import yaml
import kubernetes.client
import kubernetes.client.rest
import kubernetes.config

log = logging.getLogger(__name__)


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('output_dir', help='directory where to dump')
    arg_parser.add_argument('--in-cluster', action='store_true', help='configure with in cluster kubeconfig')
    arg_parser.add_argument('--no-clean', action='store_true', help='don\'t clean output directory on start')
    arg_parser.add_argument('--no-skip-owned', action='store_true', help='don\'t skip objects with ownerReferences')
    arg_parser.add_argument('--fast', action='store_true', help='don\'t load original YAML from server')
    arg_parser.add_argument('--format', choices=['json', 'yaml'], default='yaml')
    arg_parser.add_argument('--skip-kind', nargs='+', default=['GlobalFelixConfig'], help='skip this kind')
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
    dumper.skip_kinds = args.skip_kind
    dumper.dump_all()


class Dumper:
    def __init__(self, output_dir):
        self.client = kubernetes.client.ApiClient()
        self.output_dir = output_dir
        self.format = 'yaml'
        self.clean_output = True
        self.skip_owned = True
        self.improved_yaml = True
        self.skip_kinds = ['GlobalFelixConfig']

    def call(self, *args, **kwargs):
        return self.client.call_api(*args, **kwargs, _return_http_data_only=True)

    def dump_all(self):
        if self.clean_output:
            shutil.rmtree(self.output_dir, ignore_errors=True)

        api_groups = self.call('/apis/', 'GET', response_type='V1APIGroupList').groups
        api_group_versions = [api_group.preferred_version.group_version for api_group in api_groups]

        # v1 API group
        api_group_versions.append('v1')

        # Make this API group lowest priority
        if 'extensions/v1beta1' in api_group_versions:
            api_group_versions.remove('extensions/v1beta1')
            api_group_versions.append('extensions/v1beta1')

        dumped_kinds = set(self.skip_kinds)

        for api_group_version in api_group_versions:
            resource_path = get_api_group_version_resource_path(api_group_version)
            resources = self.call(resource_path, 'GET', response_type='V1APIResourceList').resources

            for resource in resources:
                if 'list' not in resource.verbs:
                    continue

                if resource.kind in dumped_kinds:
                    continue

                self.dump_resource(resource_path, resource)
                dumped_kinds.add(resource.kind)

    def dump_resource(self, resource_path, resource):
        list_path = '{}/{}'.format(resource_path, resource.name)
        result = self.call(list_path, 'GET', response_type=object)
        objects = result['items']

        for obj in objects:
            obj_name = obj['metadata']['name']

            if resource.namespaced:
                obj_namespace = obj['metadata']['namespace']
            else:
                obj_namespace = '_'

            log.info('%s %s/%s', resource.kind, obj_namespace, obj_name)

            if self.skip_owned and 'ownerReferences' in obj['metadata']:
                continue

            obj['apiVersion'] = result['apiVersion']
            obj['kind'] = resource.kind

            resource_dir = os.path.join(self.output_dir, obj_namespace, resource.kind)
            os.makedirs(resource_dir, exist_ok=True)

            object_filepath = os.path.join(resource_dir, obj_name)

            if self.format == 'yaml':
                with open(object_filepath + '.yaml', 'w') as fp:
                    if self.improved_yaml:
                        if resource.namespaced:
                            obj_path = '{}/namespaces/{}/{}/{}'.format(resource_path, obj_namespace, resource.name, obj_name)
                        else:
                            obj_path = '{}/{}'.format(list_path, obj_name)

                        data = self.call(obj_path, 'GET', response_type=object, header_params={
                            'Accept': 'application/yaml',
                        })

                        fp.write(data)
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
