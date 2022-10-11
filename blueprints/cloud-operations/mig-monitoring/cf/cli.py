from main import internal_check_migs
import argparse

parser = argparse.ArgumentParser(description='Process Metrics for Managed Instance Groups')
parser.add_argument('--project', type=str, required=True, help='Project to inspect')
parser.add_argument('--region', type=str, required=True, help='Region of Managed Instance Groups')
args = parser.parse_args()

enriched_instance_group_managers2 = {'instanceGroupManagers': {}, 'result': 'ok'}
internal_check_migs(args.project, args.region ,enriched_instance_group_managers2)