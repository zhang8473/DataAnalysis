import numpy as np
from datetime import datetime
from configs.ml import FEATURE_MIN_MAX_MAPPING
from lib.datatype.doc import Doc
from libquerynormalization.configs.environments import COMMON_DB_INDEX
from configs.view_keys import TALENT_VIEW_KEYS
from lib.utils.wordvecs_provider import get_industries_title_vector
from libsearcher.pylibshared.utils.logger import get_logger
logger = get_logger(__name__, 'DEBUG', to_file=False, to_stdout=True)
MAX_UNIV_RANK = FEATURE_MIN_MAX_MAPPING['talent_university_rank'][1]


class Talent(Doc):
    LOCATIONS_KEY = 'preferredLocations'


    def __init__(self, talent: dict, date_: datetime):
        """
        :param talent: raw objects gotten from Elastic get and search APIs
        :return: reformatted information ready for combining with jobs
        """
        super().__init__(talent)  # loaded id, lastModifiedDate, skills, location
        self._date = date_
        talent_dict = talent.get('_source', talent)  # _source is for elastic data, our data file is just talent
        self._from_common_db = talent.get('_index', None) == COMMON_DB_INDEX
        # load degree
        self._highest_degree_score = talent_dict.get('highestDegreeScore', np.NaN)
        if self._highest_degree_score is None:
            self._highest_degree_score = np.NaN
        # languages
        self._languages = set()
        for language in talent_dict.get('languages', []):
            # todo -- remove dict format - {start}
            if isinstance(language, dict):
                language = language["regulatedName"]
            # todo -- remove dict format - {end}
            self._languages.add(language)
            for tokenized_title in self._tokenized_titles:
                if language.replace('_', ' ').lower() in tokenized_title.lower():
                    self._skills_in_title.add(language)
        # title or major
        # for freshmen, use major name as title
        if not self._tokenized_titles:
            self._tokenized_titles = talent_dict.get('tokenizedMajors', [])
            # todo: remove this --- temp data fix {start} --- tokenizedMajors[0] is a list
            if self._tokenized_titles and isinstance(self._tokenized_titles[0], list):
                import itertools
                self._tokenized_titles = [t_ for t_ in itertools.chain(*self._tokenized_titles)]
            # --- temp data fix {end} --- tokenizedMajors[0] is a list
        # calculate title vectors
        self._title_vectors = []
        for title_ in self._tokenized_titles:
            self._title_vectors.append(get_industries_title_vector(title_))
        # load experiences
        self._best_company_class = -1
        for exp in talent_dict.get('experiences', []):
            # famous_company meaning:
            # -1: not found in wiki
            # 0: not famous
            # 1: famous
            # --- tmp code, remove after data refreshed ---{end}
            famous_company = exp.get('companyInfo', {}).get('isFamousCompany', -1)
            if famous_company > self._best_company_class:
                self._best_company_class = 1 if famous_company else 0

        # load posting date
        self._posting_date, self._last_modified_date = None, None
        created_date_string = talent_dict.get('createdDate', None)
        try:
            # last modified date might be the last status change date of the job
            if created_date_string:
                self._posting_date = datetime.strptime(created_date_string, '%Y-%m-%dT%H:%M:%SZ')
            if self._last_modified_date_string:
                self._last_modified_date = datetime.strptime(self._last_modified_date_string,
                                                             '%Y-%m-%dT%H:%M:%SZ')
                if not self._posting_date:
                    self._posting_date = self._last_modified_date
        except ValueError:
            logger.error(f'Cannot read {talent.get("_index", "???")}/{talent.get("_id", "???")}'
                         f' created/lastModified date: {created_date_string}/{self._last_modified_date_string}')

        self._current_career_years = np.NaN
        # todo remove old key recentExperienceStartDate
        try:
            self._current_career_years = (date_ -
                                          datetime.strptime(talent_dict.get('recentExperienceStartDate', talent_dict.get('recentJobFunctionStartDate', None)),
                                                            '%Y-%m-%d')
                                          ).days / 365
        except ValueError:
            logger.error(f"Cannot read recentJobFunctionStartDate:"
                         f"{talent.get('recentJobFunctionStartDate', None)} as date.")
        except TypeError:
            # if no experience, it means 0 year
            if 'experiences' not in talent_dict:
                self._current_career_years = 0
            # if there is experience but start date is not specified, this value is unavailable

        # get education information
        highest_education = None
        self._best_university_rank = MAX_UNIV_RANK
        for edu in talent_dict.get('educations', []):
            # --- tmp code, remove after data refreshed ---{start}
            university_rank = edu.get('collegeWorldRank', None)
            if not university_rank:
                # --- tmp code, remove after data refreshed ---{end}
                university_rank = edu.get('collegeInfo', {}).get('collegeWorldRank', None)
            if university_rank and university_rank < self._best_university_rank:
                self._best_university_rank = university_rank
            if highest_education:
                continue
            highest_education = edu

        # load the columns shown on UI
        for k in TALENT_VIEW_KEYS:
            if v := talent_dict.get(k, None):
                self._view[k] = list(self._skill_dict.keys()) if k == 'skills' else v
        if highest_education:
            major_names = highest_education.get('majorName', '')
            if isinstance(major_names, list) and major_names:
                highest_education['majorName'] = '|'.join(major_names)
            self._view['highestEducation'] = highest_education

    @property
    def posting_days(self):
        """
        Feature 10
        :return:
        """
        if not self.posting_date:
            return np.NaN
        else:
            posting_days = (self.investigating_date - self.posting_date).days
            return posting_days if posting_days > 0 else 0

    @property
    def investigating_date(self):
        return self._date

    @property
    def from_common_db(self):
        return self._from_common_db

    @property
    def title_vectors(self):
        return self._title_vectors

    @property
    def highest_degree_score(self):
        return self._highest_degree_score

    @property
    def posting_date(self):
        return self._posting_date

    @property
    def university_rank(self):
        return self._best_university_rank

    @property
    def best_company_class(self):
        return self._best_company_class

    @property
    def current_career_years(self):
        return self._current_career_years

    @property
    def last_modified_date_str(self) -> str:
        return self._last_modified_date_string

    @property
    def languages(self):
        return self._languages
