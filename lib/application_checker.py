import os
from libsearcher import ConditionsCollection, Operator
from libsearcher.location import LocationSearcher, SearchMode
from libsearcher.pylibshared.datatype import LocationInfo, OfficialLocationInfo
from libsearcher.pylibshared.utils.string_utils import list_in_list
from libsearcher.pylibshared.utils.elastic import ESClient
from libsearcher.pylibshared.utils.logger import get_logger
logger = get_logger(__name__, debug_level='DEBUG', to_file=True, to_stdout=True)
target_es = ESClient(hosts=os.environ.get('ELASTIC_HOSTS', 'localhost'), timeout=120)
TALENT_INDEX = 'talents_recommendation'


class ApplicationChecker:
    def __init__(self, talent, job):
        self._talent = talent
        self._job = job
        target_es.push_doc(index_=TALENT_INDEX, doc_=talent, id_="recommend_talent")
        country_searchers = []
        for job_location in self._job['locations']:
            if country := job_location.get('officialCountry', None):
                loc = LocationInfo(OfficialLocationInfo(country=country))
            elif country := job_location.get('country', None):
                loc = LocationInfo(country=country)
            else:
                continue
            country_searchers.append(loc)
        country_condition = ConditionsCollection(operator=Operator.should,
                                                 conditions=[LocationSearcher(search_mode=SearchMode.PREFERRED, )])

    def check(self):



def compare_lists(la, lb, get_all=False):
    all_matched = []
    for a in la:
        if sa := a.split(' '):
            for b in lb:
                if (sb := b.split(' ')) and list_in_list(sa, sb, minimum_should_match='2'):
                    if not get_all:
                        return a, b
                    else:
                        all_matched.append((a, b))
    return all_matched
