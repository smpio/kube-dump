import os
import json
import shutil
import logging

import yaml
import kubernetes.client
import kubernetes.client.rest
import kubernetes.config

from . import config

log = logging.getLogger(__name__)


def main():
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

    if config.in_cluster:
        kubernetes.config.load_incluster_config()
    else:
        kubernetes.client.configuration.host = 'http://127.0.0.1:8001'

    dumper = Dumper()
    dumper.dump_all()


class Dumper:
    def __init__(self):
        self.client = kubernetes.client.ApiClient()
        self.output_dir = 'dump'
        self.use_yaml = True
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
            if self.skip_owned and 'ownerReferences' in obj['metadata']:
                continue

            obj['apiVersion'] = result['apiVersion']
            obj['kind'] = resource.kind

            obj_name = obj['metadata']['name']
            if resource.namespaced:
                obj_namespace = obj['metadata']['namespace']
            else:
                obj_namespace = '_'

            resource_dir = os.path.join(self.output_dir, obj_namespace, resource.kind)
            os.makedirs(resource_dir, exist_ok=True)

            object_filepath = os.path.join(resource_dir, obj_name)

            if self.use_yaml:
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
            else:
                with open(object_filepath + '.json', 'w') as fp:
                    json.dump(obj, fp, indent=2)


def get_api_group_version_resource_path(api_group_version):
    if api_group_version == 'v1':
        return '/api/v1'
    else:
        return '/apis/' + api_group_version
