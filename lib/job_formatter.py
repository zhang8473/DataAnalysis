import json
from libsearcher.pylibshared.enumerations.degrees import degree_regulator
from libsearcher.pylibshared.enumerations.languages import extract_languages
from libsearcher.pylibshared.enumerations.levels import JobExperienceLevels, maxsize
from libsearcher.pylibshared.utils.logger import get_logger
logger = get_logger(__name__, 'INFO', to_file=True, to_stdout=True)


def separate_keywords(keys):
    degrees, langs, skills = [], [], []
    for k_ in keys:
        k_ = k_.strip()
        d_, _, _ = degree_regulator(k_)
        if d_:
            degrees.append(d_)
        elif extract_languages(k_):
            langs.append(k_)
        else:
            skills.append(k_)
    return degrees, langs, skills


def get_list(raw: str):
    if not raw:
        return []
    try:
        return json.loads(raw)
    except json.decoder.JSONDecodeError:
        return raw.split(',')


def fill_regulated_experience_yrs(doc):
    least_experience_level_yr, most_experience_level_yr = None, None
    self_least_experience_level_yr = doc.pop('leastExperienceYear', None)
    self_most_experience_level_yr = doc.pop('mostExperienceYear', None)
    for level_str in doc.pop('expLevels', []):
        try:
            yr_range = JobExperienceLevels.__getattr__(level_str).value
            if not least_experience_level_yr or yr_range[0] < least_experience_level_yr:
                # use Parser parsed value if not conflict with expLevels
                if self_least_experience_level_yr and \
                        yr_range[0] <= self_least_experience_level_yr <= yr_range[1]:
                    least_experience_level_yr = self_least_experience_level_yr
                else:
                    least_experience_level_yr = yr_range[0]
            if not most_experience_level_yr or yr_range[1] > most_experience_level_yr:
                if self_most_experience_level_yr and \
                        yr_range[0] <= self_most_experience_level_yr <= yr_range[1]:
                    most_experience_level_yr = self_most_experience_level_yr
                else:
                    most_experience_level_yr = yr_range[1]
        except AttributeError:
            logger.error(f"Received unknown experience level {level_str}")
    experience_yr = {}  # empty means any experience
    if least_experience_level_yr is not None and least_experience_level_yr >= 0:
        experience_yr['gte'] = least_experience_level_yr
    if most_experience_level_yr is not None and most_experience_level_yr < maxsize:
        experience_yr['lte'] = most_experience_level_yr
    if experience_yr:
        doc['experienceYearRange'] = experience_yr
    return doc


def job_v1_to_v2(doc):
    # experience years
    fill_regulated_experience_yrs(doc)
    # skill
    required_skills = []
    preferred_skills = []
    skills = doc.pop('skills', [])
    max_skill_score = 1
    for skill_obj in skills:
        if (score := skill_obj.get('score', -1)) > max_skill_score:
            max_skill_score = score
    for skill_obj in skills:
        if score := skill_obj.get('score', None):
            skill_obj['score'] = score / max_skill_score
    degrees = []
    for s_ in doc.get('preferredDegrees', []) + [doc.get('minimumDegree', None)]:
        d, _, _ = degree_regulator(s_)
        if d:
            degrees.append(d)
    if r_keys_ := get_list(doc.pop('requiredKeywords', [])):
        degrees_, r_langs, r_skills = separate_keywords(r_keys_)
        degrees_.extend(degrees_)
        for r_skill in r_skills:
            for skill in skills:
                if skill['skillName'] == r_skill:
                    skill.pop('id')
                    skill.pop('necessity')
                    required_skills.append(skill)
                    break
            else:
                required_skills.append({'skillName': r_skill})
        if r_langs:
            doc['requiredLanguages'] = doc.get('requiredLanguages', []) + r_langs
    if keys_ := get_list(doc.pop('keywords', [])):
        degrees_, p_langs, p_skills = separate_keywords([k for k in keys_ if k not in r_keys_])
        degrees.extend(degrees_)
        for p_skill in p_skills:
            for skill in skills:
                if skill['skillName'] == p_skill:
                    skill.pop('id')
                    skill.pop('necessity')
                    preferred_skills.append(skill)
                    break
            else:
                preferred_skills.append({'skillName': p_skill})
        if p_langs:
            doc['preferredLanguages'] = doc.get('preferredLanguages', []) + p_langs
    if required_skills:
        doc['requiredSkills'] = required_skills
    if preferred_skills:
        doc['preferredSkills'] = preferred_skills
    if degrees:
        r_d = min(degrees, key=lambda d: d.score)
        doc['minimumDegreeLevel'] = r_d.name
        ds = set(degrees)
        ds.discard(r_d)
        doc['preferredDegrees'] = [d.name for d in ds]
    doc.pop('id')
    # doc.pop('primarySale', None)
    doc.pop('priority', None)
    doc.pop('boolstr', None)
    # users
    assigned_users = {}
    names = []
    if d_ := doc.pop('primaryRecruiter', None):
        assigned_users['PR'] = [str(d_['id'])]
        names.append(d_['firstName'] + ' ' + d_['lastName'])
    if assigned_users:
        assigned_users['names'] = names
        doc['assignedUsers'] = assigned_users
    # company
    if s_ := doc.pop('company', None):
        doc['companyName'] = s_
    if s_ := doc.pop('companyId', None):
        doc['companyId'] = str(s_)
    # pay
    factor = 1
    if s_ := doc.pop('payRateUnitType'):
        if s_ == 'HOURLY':
            factor = 2000
        elif s_ == 'DAYLY':
            factor = 250
        elif s_ == 'WEEKLY':
            factor = 50
        elif s_ == 'MONTHLY':
            factor = 12
        if doc['jobType'] == 'FULL_TIME':
            doc['payType'] = 'Y'
        else:
            doc['payType'] = s_[0]
    bill_range = {}
    if s_ := doc.pop('billRateFrom', None):
        bill_range['gte'] = s_ * factor
    if s_ := doc.pop('billRateTo', None):
        bill_range['lte'] = s_ * factor
    if bill_range and doc.get('currency') == 'USD':
        doc['billRangeInUSD'] = bill_range
    pay_range = {}
    if s_ := doc.pop('payRateFrom', None):
        pay_range['gte'] = s_ * factor
    if s_ := doc.pop('payRateTo', None):
        pay_range['lte'] = s_ * factor
    if pay_range and doc.get('currency') == 'USD':
        doc['salaryRangeInUSD'] = bill_range
    # location
    location = {}
    for k in ('city', 'province', 'zipcode', 'country'):
        if s_ := doc.pop(k, None):
            location[k] = s_
    if location:
        doc['locations'] = [location]
    # text
    if s_ := doc.pop('jdText', None):
        doc['text'] = s_
    return doc
