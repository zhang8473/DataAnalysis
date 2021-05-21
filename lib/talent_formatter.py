import csv
import codecs

INDUSTRY_MAPPING = {}
with codecs.open("meta_data/Linkedin-HiTalent Industry Mapping.csv", 'rU', 'utf-8-sig') as csv_file:
    for row_ in csv.reader(csv_file, delimiter=r',', quotechar=r'"', quoting=csv.QUOTE_MINIMAL):
        INDUSTRY_MAPPING[row_[0].lower()] = set()
        for ind_ in row_[1].split('|'):
            words = ind_.split('.')
            for i in range(len(words)):
                INDUSTRY_MAPPING[row_[0].lower()].add('.'.join(words[:i + 1]))


def talent_v1_to_v2(doc):
    industries = set()
    for key in doc.get('industries', []):
        industries |= INDUSTRY_MAPPING[key.lower()]
    if industries:
        doc['industries'] = list(industries)
    return doc
