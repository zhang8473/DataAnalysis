import re
from geopy.point import Point

from libsearcher.pylibshared.datatype import LocationInfo, OfficialLocationInfo
from libsearcher.utils.title_synonym import title_synonym
# from configs.view_keys import COMMON_VIEW_KEYS
from libsearcher.pylibshared.enumerations import Levels
from libsearcher.pylibshared.enumerations.location.country import get_country
# from lib.utils.wordvecs_provider import get_skills_vector
from libsearcher.pylibshared.utils.string_utils import list_in_list
from libsearcher.pylibshared.utils.logger import get_logger
logger = get_logger(__name__, 'INFO', to_file=True, to_stdout=True)
TIME_REGULATOR_REGEX = re.compile(r'\.[0-9]*Z')


class Doc:
    LOCATIONS_KEY = None

    def __init__(self, doc: dict):
        """
        :param doc: raw object gotten from Elastic get and search APIs
        :return: reformatted information ready for combining with talents
        """
        # index and id
        if _id := doc.get('_id', None):
            self._id = str(_id)
            _index = doc.get('_index', None)
        else:
            raise ValueError(f"Doc _id is missing: {doc}")
        if not (_source := doc.get('_source', None)):
            raise ValueError(f"Doc _source is missing: {doc}")
        logger.debug(f"Building {_index}/{self._id}...")
        # job function and industries
        self._job_functions = _source.get('jobFunctions', [])
        self._industries = _source.get('industries', [])
        # load location
        self._locations = [LocationInfo.create_from_dict(l_) for l_ in _source.get(self.LOCATIONS_KEY, [])]
        self._countries = [LocationInfo(country=l_.get('country', ""),
                                        official_loc=OfficialLocationInfo(l_.get('officialCountry', "")))
                           for l_ in _source.get(self.LOCATIONS_KEY, [])]
        self._tokenized_titles = []

        tokenized_titles = _source.get('tokenizedTitles', [])
        for tokenized_title in tokenized_titles:
            for title_ in title_synonym.get_title_synonyms(tokenized_title):
                self._tokenized_titles.append(title_)
        try:
            level_str = _source.get('level', "JUNIOR")
            self._level_score = Levels.__getattr__(level_str).score
        except AttributeError:
            self._level_score = None
            logger.error(f"Received unknown title level {_source.get('level')}")
        # load skills
        # to be compatible with the old version, still load tokenizedSkills here
        self._skill_dict = {}
        regulated_names = []
        for skill in _source.get('skills', []):
            if skill_name := skill.pop('skillName', None):
                skill.pop('lastModifiedDate', None)
                self._skill_dict[skill_name] = skill

        # self._skill_vectors = get_skills_vector(regulated_names)
        # initialize titles with the level(current) titles
        logger.debug(f"Title: {self._tokenized_titles}, level={self._level_score}")
        # if prepare_for_search:
        #     self.__prepare_for_search(doc_dict)
        # copy the keys that will be shown in UI
        self._view = {'esId': self._id, 'index': _index}
        # for k in COMMON_VIEW_KEYS:
        #     v = doc_dict.get(k, None)
        #     if not v:
        #         continue
        #     self._view[k] = v

    def __prepare_for_search(self, doc_dict):
        self._company = doc_dict.get('company', None)
        self._regulated_company_name = doc_dict.get('companyInfo', {}).get("regulatedCompanyName", None)

    def __repr__(self):
        return f"{self._view}"

    def __str__(self):
        return f"{self._view}"

    @property
    def id(self):
        return self._id

    @property
    def regulated_company_name(self):
        return self._regulated_company_name

    @property
    def company(self):
        return self._company

    @property
    def tokenized_titles(self):
        """
        if talent has working experience, it will be his/her most recent titles
        for freshmen, it is the highest and latest major name
        :return:
        """
        return self._tokenized_titles

    @property
    def job_functions(self):
        return self._job_functions

    @property
    def job_function_subs(self):
        return self._job_function_subs

    @property
    def level_score(self):
        """
        :return: level scores, its length must be the same as the title vector length
        """
        return self._level_score

    @property
    def industries(self):
        return self._industries

    @property
    def industry_vector(self):
        return None

    @property
    def last_modified_date_str(self) -> str:
        return self._last_modified_date_string

    @property
    def skill_dict(self):
        return self._skill_dict

    # @property
    # def skill_vectors(self):
    #     return self._skill_vectors

    @property
    def locations(self):
        return self._locations

    @property
    def view(self):
        return self._view
