import argparse
import json
import pprint
import pandas as pd
from lib.connectors import APN, MariaDBFetcher, ESFiller
from lib.datatype.job import Job
from lib.job_formatter import job_v1_to_v2
from lib.talent_formatter import talent_v1_to_v2
from libsearcher.pylibshared.utils.elastic import ESClient
from libsearcher import TalentConditions, KeywordSearcher, Operator
from libsearcher.pylibshared.utils.logger import get_logger
pp = pprint.PrettyPrinter(indent=4, width=200)
logger = get_logger(__name__, debug_level='DEBUG', to_file=True, to_stdout=True)
TARGET_ES = ESClient("localhost:9200")
CHECKERS = ('Amos', 'Shirley')
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
        self.__all_conditions = conditions
        self.__check_queue = [self.__all_conditions]
        self.__success_conditions = []
        self.__bfs_conditions()

    def get_result(self):
        for cond_ in self.__all_conditions:
            if cond_ not in self.__success_conditions:
                print(f"Dropped condition: {cond_.ui_json} - {cond_.es_condition}")
        return self.__success_conditions

    def __bfs_conditions(self):
        while self.__check_queue:
            current_conditions = self.__check_queue.pop(0)
            if self.__check_conditions(current_conditions):
                self.__success_conditions = current_conditions
                return True
            if len(current_conditions) > 1:
                for i in range(len(current_conditions)):
                    self.__check_queue.append(current_conditions[:i] + current_conditions[i+1:])
        return False

    def __check_conditions(self, conditions):
        id_search = KeywordSearcher(keywords=[self.__id], key='_id')
        all_conditions = TalentConditions(conditions=conditions + [id_search], operator=Operator.must)
        if f_ := all_conditions.invalid_fields:
            print('Invalid Field:', json.dumps(f_, indent=4))
            return
        es_condition = all_conditions.es_condition
        if TARGET_ES.count(index=self.__index, body={"query": es_condition}):
            return True
        else:
            return False


def main():
    checked_data = CheckedData('data/训练数据修正 - application_202005251554.csv')
    apn = APN(host='https://api.hitalentech.com', refresh_token=args.refresh_token)
    es_filler = ESFiller()
    for row in checked_data.fetch():
        job_apn_json = job_v1_to_v2(apn.get_job(row.job_id))
        job_filled = es_filler.fill_job(job_apn_json, row.job_id)
        talent_apn_json = talent_v1_to_v2(apn.get_talent(row.talent_id))
        talent_filled = es_filler.fill_talent(talent_apn_json, row.talent_id)
        pp.pprint(job_filled)
        j_ = Job({"_id": "dummy", "_index": "dummy", "_source": job_filled})
        conditions_ = j_.get_required_conditions()
        checker = ConditionChecker('talents_10', row.talent_id, conditions_)
        print(checker.get_result())
        # pp.pprint(job_filled)
        # pp.pprint(talent_filled)
        break
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
