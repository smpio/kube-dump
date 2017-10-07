import os

in_cluster = os.environ.get('IN_CLUSTER', '1').lower() in ('1', 'true', 'yes')
