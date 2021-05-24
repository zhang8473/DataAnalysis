import numpy as np
from datetime import datetime
from libsearcher import TalentConditions, KeywordSearcher, Operator, ExperienceYearSearcher, LanguageSearcher, JobFunctionSearcher, DegreeSearcher, LocationSearcher
from libsearcher.location import SearchMode as LocationSearchMode
from libsearcher.degree import SearchMode as DegreeSearchMode
from libsearcher.pylibshared.enumerations import JobStatus
from libsearcher.pylibshared.datatype import LocationInfo
# from configs.view_keys import JOB_VIEW_KEYS
# from lib.utils.wordvecs_provider import get_industries_title_vector
from lib.datatype.doc import Doc
from libsearcher.pylibshared.utils.logger import get_logger
logger = get_logger(__name__, 'DEBUG', to_file=True, to_stdout=True)


class Job(Doc):
    LOCATIONS_KEY = 'locations'

    def __init__(self, job: dict):
        """
        :param job: raw object gotten from Elastic get and search APIs
        :return: reformatted information ready for combining with talents
        """
        super().__init__(job)  # loaded id, lastModifiedDate, skills, location
        job_dict = job.get('_source', None)  # _source is for elastic data, our data file is just talent
        # load languages
        self._required_languages = job_dict.get('requiredLanguages', [])
        self._preferred_languages = job_dict.get('preferredLanguages', [])
        self._required_degree = self._get_degree(job_dict.get('minimumDegreeLevel', None))
        self._bool_obj = job_dict.get('boolObj', [])   # boolean object for skill search
        self._exp_range = job_dict.get('experienceYearRange', {})
        self._job_functions = job_dict.get('jobFunctions', [])
        self._min_exp, self._max_exp = self._exp_range.get('gte', np.NaN), self._exp_range.get('lte', np.NaN)

    def get_required_conditions(self):
        skill_ = TalentConditions.create_from_ui_json(self._bool_obj) if self._bool_obj else None
        location_ = TalentConditions(conditions=[
            LocationSearcher(search_mode=LocationSearchMode.PREFERRED, location_info=loc) for loc in self._countries
        ], operator=Operator.should)
        if (tolerance_min := self._exp_range.get('gte', self._exp_range.get('gt', 0)) // 3) > 2:
            tolerance_min = 2
        if (tolerance_max := self._exp_range.get('lte', self._exp_range.get('lt', 0)) // 3) > 3:
            tolerance_max = 3
        exp_ = ExperienceYearSearcher(range_=self._exp_range,
                                      # e.g. require 6-9 years, we search 4-12 years
                                      # e.g. require 2-6 years, we search 2-9 years
                                      tolerance_min=tolerance_min,
                                      tolerance_max=tolerance_max) if self._exp_range else None
        language_ = TalentConditions(
            conditions=[LanguageSearcher(keywords=[lang_], key='languages') for lang_ in self._required_languages],
            operator=Operator.must
        )
        jf_ = JobFunctionSearcher(keywords=self._job_functions, key='jobFunctions')
        if self._required_degree:
            degree_ = DegreeSearcher(degrees=[self._required_degree], search_mode=DegreeSearchMode.NOT_UNDER)
        else:
            degree_ = None
        return list(filter(None, [skill_, location_, exp_, language_, jf_, degree_]))

    def __get_end_date(self, end_date_string, last_activity_time_string):
        try:
            if end_date_string:
                return datetime.strptime(end_date_string, '%Y-%m-%d')
            if last_activity_time_string:
                return datetime.strptime(last_activity_time_string.split('T')[0], '%Y-%m-%d')
            if self._last_modified_date_string and self._status not in (JobStatus.Open, JobStatus.Reopened):
                return datetime.strptime(self._last_modified_date_string.split('T')[0], '%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            logger.warn(f'Cannot read job end date: '
                        f'{end_date_string}/{last_activity_time_string}/{self._last_modified_date_string}')

    def __repr__(self):
        return f"{self._view}"

    def __str__(self):
        return f"{self._view}"

    @property
    def id(self):
        return self._id

    @property
    def title_vector(self):
        return self._title_vector

    @property
    def boolstr(self):
        return self._bool_obj

    @property
    def experience_yr_range(self):
        return self._experience_yr_range

    @property
    def preferred_degree_scores(self):
        return self._preferred_degree_scores

    @property
    def minimum_degree_score(self):
        return self._minimum_degree_score

    @property
    def ending_date(self):
        return self._end_date

    @property
    def posting_date(self):
        return self._posting_date

    @property
    def last_modified_date_str(self) -> str:
        return self._last_modified_date_string

    @property
    def skill_vectors(self):
        return self._skill_vectors

    @property
    def view(self):
        return self._view

    @property
    def preferred_languages(self):
        return self._preferred_languages

    @property
    def required_languages(self):
        return self._required_languages


def get_posting_date(job_dict: dict):
    """
    :param job_dict:
    :return:
    """
    # load posting date
    posting_date_string = job_dict.get('postingTime', None)
    try:
        if posting_date_string:
            return datetime.strptime(posting_date_string, '%Y-%m-%dT%H:%M:%SZ')
        # last modified date might be the last status change date of the job
        posting_date_string = job_dict.get('createdDate', None)
        if posting_date_string:
            return datetime.strptime(posting_date_string, '%Y-%m-%dT%H:%M:%SZ')
        return None
    except ValueError:
        try:
            return datetime.strptime(posting_date_string, '%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            logger.warn('Cannot read job posting date: {0}'.format(posting_date_string))
