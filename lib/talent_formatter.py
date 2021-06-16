import csv
import codecs
from libsearcher.pylibshared.utils.logger import get_logger

logger = get_logger(__name__, debug_level='DEBUG', to_file=True, to_stdout=True)

INDUSTRY_MAPPING = {}
with codecs.open("meta_data/Linkedin-HiTalent Industry Mapping.csv", 'rU', 'utf-8-sig') as csv_file:
    for row_ in csv.reader(csv_file, delimiter=r',', quotechar=r'"', quoting=csv.QUOTE_MINIMAL):
        INDUSTRY_MAPPING[row_[0].lower()] = set()
        for ind_ in row_[1].split('|'):
            words = ind_.split('.')
            for i in range(len(words)):
                INDUSTRY_MAPPING[row_[0].lower()].add('.'.join(words[:i + 1]))


def talent_v1_to_v2(doc):
    # contact
    contacts = doc.pop('contacts', None)
    if website := doc.pop('website', None):
        if 'linkedin' in website.lower():
            for contact in contacts:
                if contact.get('type', None) == "LINKEDIN" and contact.get('details', None) != website:
                    raise ValueError(f"Inconsistent LinkedIn {website} -- {contact.get('details', None)}")
    # industries
    industries = set()
    for key in doc.get('industries', []):
        industries |= INDUSTRY_MAPPING[key.lower()]
    if industries:
        doc['industries'] = list(industries)
    # useless keys
    for key in ('active', 'chinese', 'photoUrl', 'esId', 'jobFunctions',
                'preferredLocations', 'notes', 'ownerships'):
        doc.pop(key, None)
    # salary
    preferred_salary_range = {}
    if sf_ := doc.pop('expectedSalaryFrom', None):
        preferred_salary_range["gte"] = sf_
    if st_ := doc.pop('expectedSalaryTo', None):
        preferred_salary_range["lte"] = st_
    if preferred_salary_range:
        logger.warn("Unknown currency for expected salary")
    # experience
    title = doc.pop('title', None)
    company = doc.pop('company', None)
    if title or company:
        for exp in doc.get('experiences', []):
            if exp.get('current', None):
                exp.pop('endDate', None)
            if location := exp.get('workLocation', None):
                exp['location'] = location
        for exp in doc.get('experiences', []):
            if title == exp.get('title', None) and company == exp.get('company', None):
                break
        else:
            logger.warn(f'Title \"{title}\" does not exist in experiences.')
            if 'experiences' not in doc:
                doc['experiences'] = []
            exp = {}
            if title:
                exp['title'] = title
            if company:
                exp['companyName'] = company
            doc['experiences'].append(exp)
    # skills
    for skill in doc.get('skills', []):
        skill.pop('score', None)
        skill.pop('necessity', None)
        if skill.pop('current', None):
            skill.pop('lastUsed', None)
            skill.pop('usedMonth', None)
        elif skill.get('lastUsed', None):
            skill.pop('firstUsed', None)
        else:
            skill.pop('current', None)
            skill.pop('usedMonth', None)
    return doc
