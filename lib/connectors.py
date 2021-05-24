import json
import mariadb
from environments import JOB_USELESS_KEYS, TALENT_USELESS_KEYS
from libsearcher.pylibshared.utils.logger import get_logger
logger = get_logger(__name__, debug_level='DEBUG', to_file=True, to_stdout=True)


class MariaDBFetcher:
    def __init__(self, server, user, password, db):
        host, port = server.split(':')
        print(f"Connect to {host}:{port}, db={db}")
        try:
            self.__conn = mariadb.connect(
                user=user,
                password=password,
                host=host,
                port=int(port),
                database=db

            )
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
            raise e

    def fetch(self, skip=0):
        cur = self.__conn.cursor()
        # t.`status`, t.tenant_id, t.created_date,
        cur.execute(
            """
            SELECT t.talent_id, t.job_id, 
            CASE (SELECT 1 from activity_unique a WHERE a.application_id = t.id AND a.`status` = 4) WHEN 1 THEN 'True' ELSE 'False' END isInterviewed
            FROM application t WHERE t.tenant_id = 4 AND t.job_id not in (1197,1198,1196) and t.status in (4,5,6,7,9,12,13) and t.created_date > "2018-06-01" ;
            """)
        for sample in cur:
            if skip <= 0:
                yield sample
            else:
                skip -= 1


GENERAL_HEADER = {'Content-Type': 'application/json', 'charset': 'UTF-8'}


class APN:
    def __init__(self, host, refresh_token):
        self.__host = host
        self.__refresh_token = refresh_token
        self.__header = GENERAL_HEADER.copy()
        self.__token_renewed = False

    def __renew_access_token(self):
        """
        https://api.hitalentech.com/api/v1/refresh-token
        :return:
        """
        response_ = requests.request("POST", self.__host + '/api/v1/refresh-token',
                                     headers=GENERAL_HEADER,
                                     data=json.dumps({"refresh_token": self.__refresh_token}).encode('utf-8'))
        logger.debug(f"Refreshed token: {response_.text}")
        self.__header['Authorization'] = rf'Bearer {response_.json()["access_token"]}'
        self.__token_renewed = True

    @staticmethod
    def __format_job_data(data):
        if isinstance(data, dict):
            data = dict((k, v_) for k, v in data.items() if k not in JOB_USELESS_KEYS
                        and (v_ := APN.__format_job_data(v)) is not None)
        elif isinstance(data, list):
            data = list(v_ for v in data if (v_ := APN.__format_job_data(v)) is not None)
        if data in (None, 'None', '', [], {}):
            return None
        if isinstance(data, str):
            data = data.strip()
        return data

    @staticmethod
    def __format_talent_data(data):
        if isinstance(data, dict):
            data = dict((k, v_) for k, v in data.items() if k not in TALENT_USELESS_KEYS
                        and (v_ := APN.__format_talent_data(v)) is not None)
        elif isinstance(data, list):
            data = list(v_ for v in data if (v_ := APN.__format_talent_data(v)) is not None)
        if data in (None, 'None', '', [], {}):
            return None
        if isinstance(data, str):
            data = data.strip()
        return data

    def get_talent(self, id_):
        if not self.__token_renewed:
            self.__renew_access_token()
        response = requests.request("GET", self.__host + f'/api/v1/talents/{id_}', headers=self.__header)
        response_body = response.json()
        if 'error' in response_body:
            raise ConnectionResetError(f"{response_body}")
        return APN.__format_talent_data(response_body)

    def get_job(self, id_):
        if not self.__token_renewed:
            self.__renew_access_token()
        response = requests.request("GET", self.__host + f'/api/v1/jobs/{id_}', headers=self.__header)
        response_body = response.json()
        if 'error' in response_body:
            raise ConnectionResetError(f"{response_body}")
        return APN.__format_job_data(response_body)


import requests


class ESFiller:
    def __init__(self, server='localhost:5050'):
        self._address = 'http://' + server

    def fill_job(self, doc, id_):
        response = requests.request("POST",
                                    url=self._address + f'/filler/v2/sync/tenant/10/job/{id_}/fill_es/',
                                    headers={'Content-Type': 'application/json'},
                                    data=json.dumps(doc, ensure_ascii=False).encode('utf-8'))
        return response.json()

    def fill_talent(self, doc, id_):
        response = requests.request("POST",
                                    url=self._address + f'/filler/v1/sync/tenant/10/talent/{id_}/fill_es/',
                                    headers={'Content-Type': 'application/json'},
                                    data=json.dumps(doc, ensure_ascii=False).encode('utf-8'))
        return response.json()
