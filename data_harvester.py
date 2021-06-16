import time
import argparse
import json
import pprint
import pandas as pd
from lib.connectors import APN, MariaDBFetcher, ESFiller
from lib.datatype.job import Job
from lib.job_formatter import job_v1_to_v2
from lib.talent_formatter import talent_v1_to_v2
from libsearcher.pylibshared.utils.elastic import ESClient, DocNotFoundError
from libsearcher import TalentConditions, KeywordSearcher, Operator
from libsearcher.pylibshared.utils.logger import get_logger
pp = pprint.PrettyPrinter(indent=4, width=200)
logger = get_logger(__name__, debug_level='DEBUG', to_file=True, to_stdout=True)
TARGET_ES = ESClient("localhost:9200")
TENANT_ID = '10'
CHECKERS = ('Amos',)
SELECT_COLUMNS = {"talent_id": 'int64', "job_id": 'int64'}


class CheckedData:
    def __init__(self, filename):
        self._df = pd.read_csv(filename, delimiter=',', usecols=[0, 1, 2, 8, 9],
                               dtype={"talent_id": 'int64', "job_id": 'int64'})
        self._df = self._df[self._df.job_duty.isin(CHECKERS) & self._df.talent_duty.isin(CHECKERS)]
        # self._df = self._df[self._df.interviewed].filter(items=('talent_id', 'job_id'))
        print(self._df[:5])
        print(self._df[-5:])
        print(self._df.shape)

    def fetch(self):
        for row in self._df.itertuples(index=False):
            yield row


class ConditionChecker:
    def __init__(self, index_, id_, conditions):
        self.__index = index_
        self.__id = str(id_)
        self.__all_conditions = list(filter(None, conditions)) if conditions else None

    def find_unsatisfied_conditions(self):
        if self.__check_conditions(self.__all_conditions):
            return None
        unsatisfied_conditions = []
        for cond_ in self.__all_conditions:
            if not self.__check_conditions([cond_]):
                if hasattr(cond_, '_operator') and cond_._operator in (Operator.must, Operator.filter) and len(cond_._conditions) > 1:
                    for cond__ in cond_._conditions:
                        unsatisfied_conditions.append(cond__)
                else:
                    unsatisfied_conditions.append(cond_)
        return unsatisfied_conditions

    def __check_conditions(self, conditions):
        id_search = KeywordSearcher(keywords=[self.__id], key='_id')
        all_conditions = TalentConditions(conditions=conditions + [id_search], operator=Operator.must)
        if f_ := all_conditions.invalid_fields:
            raise ValueError('Invalid Field:', json.dumps(f_, indent=4))
        es_condition = all_conditions.es_condition
        if TARGET_ES.count(index=self.__index, body={"query": es_condition}):
            return True
        else:
            return False


def main():
    checked_data = CheckedData('data/训练数据修正 - application_202005251554.csv')
    apn = APN(host='https://api.hitalentech.com', refresh_token=args.refresh_token)
    es_filler = ESFiller()
    i = 0
    for row in checked_data.fetch():
        i += 1
        if i < 0:
            continue
        try:
            j_ = TARGET_ES.get_doc('jobs_' + TENANT_ID, str(row.job_id))
            job_filled = j_['_source']
            if 'error' in job_filled:
                TARGET_ES.delete('jobs_' + TENANT_ID, str(row.job_id))
                raise DocNotFoundError(f"{i} - job {row.job_id} - {job_filled} doc broken")
            # logger.debug(f"Job {row.job_id} is already in ES.")
        except DocNotFoundError:
            job_apn_json = job_v1_to_v2(apn.get_job(row.job_id))
            job_filled = es_filler.fill_job(job_apn_json, row.job_id)
            j_ = {"_id": "dummy", "_index": "dummy", "_source": job_filled}
        try:
            t_ = TARGET_ES.get_doc('talents_' + TENANT_ID, str(row.talent_id))
            talent_filled = t_['_source']
            if 'error' in talent_filled:
                TARGET_ES.delete('talents_' + TENANT_ID, str(row.talent_id))
                raise DocNotFoundError(f"{i} - talent {row.talent_id} - {talent_filled} doc broken")
            # logger.debug(f"Talent {row.talent_id} is already in ES.")
        except DocNotFoundError:
            talent_apn_json = talent_v1_to_v2(apn.get_talent(row.talent_id))
            try:
                talent_filled = es_filler.fill_talent(talent_apn_json, row.talent_id)
            except Exception as e:
                pp.pprint(talent_apn_json)
                raise e
            time.sleep(1)
        conditions_ = Job(j_).get_required_conditions()
        checker = ConditionChecker('talents_' + TENANT_ID, row.talent_id, conditions_)
        if cs_ := checker.find_unsatisfied_conditions():
            logger.warning(
                f"{i} - job {row.job_id}({row.job_duty}) - talent {row.talent_id}({row.talent_duty}). "
                f"Job requirements are not satisfied:\n"
                f"{[c_.ui_json for c_ in cs_]}\n")
            # pp.pprint(job_filled)
            # pp.pprint(talent_filled)
            # break
        else:
            logger.info(f"{i} - job {row.job_id}({row.job_duty}) - talent {row.talent_id}({row.talent_duty}) has passed check.")
    # server, db = args.input.split('/')
    # fetcher = MariaDBFetcher(server=server, user=args.username, password=args.password, db=db)
    # for sample in fetcher.fetch(skip=0):
    #     talent_id, job_id, interviewed = sample
    #     interviewed = interviewed.lower() == 'true'
    #     # talent_apn_json = talent_v1_to_v2(apn.get_talent(talent_id))
    #     # pp.pprint(talent_apn_json)
    #     job_apn_json = job_v1_to_v2(apn.get_job(2840))
    #     pp.pprint(job_apn_json)
    #     break


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
    # pp.pprint(cond_.es_condition)
    # pp.pprint(cond_.invalid_fields)
