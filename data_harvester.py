import argparse
import pprint
from lib.connectors import APN, MariaDBFetcher
from lib.formatter import job_v1_to_v2
from pylibshared.utils.logger import get_logger
pp = pprint.PrettyPrinter(indent=4, width=200)
logger = get_logger(__name__, debug_level='DEBUG', to_file=True, to_stdout=True)


def main():
    server, db = args.input.split('/')
    fetcher = MariaDBFetcher(server=server, user=args.username, password=args.password, db=db)
    apn = APN(host='https://api.hitalentech.com', refresh_token=args.refresh_token)
    for sample in fetcher.fetch(skip=200):
        talent_id, job_id, interviewed = sample
        interviewed = interviewed.lower() == 'true'
        talent_apn_json = apn.get_talent(talent_id)
        pp.pprint(talent_apn_json)
        job_apn_json = apn.get_job(job_id)
        job_v1_to_v2(job_apn_json)
        pp.pprint(job_apn_json)
        break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ESfill the elasticsearch data.\n'
                                                 'python3 scripts/talents_esfill_cloud.py '
                                                 '-i localhost:3306/apn -u ? -p ?')
    parser.add_argument("-i", f"--input", type=str, help=f'input_index: the source db, e.g. localhost:3306/apn;')
    parser.add_argument("-u", f"--username", type=str, help=f'output_index: the target ES index;')
    parser.add_argument("-p", f"--password", type=str)
    parser.add_argument("-rt", "--refresh_token", type=str, help='refresh token;')
    args = parser.parse_args()
    main()
