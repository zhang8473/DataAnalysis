def job_v1_to_v2(doc):
    doc.pop('id')
    doc.pop('primarySale', None)
    doc.pop('priority', None)
    doc.pop('boolstr', None)
    # users
    assigned_users = {}
    if d_ := doc.pop('primaryRecruiter', None):
        assigned_users['PR'] = [str(d_['id'])]
        assigned_users['AM'] = [str(d_['id'])]
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
    # skill
    required_skills = []
    preferred_skills = []
    for skill in doc.pop('skills', []):
        skill.pop('id')
        if skill.pop('necessity') == 'REQUIRED':
            required_skills.append(skill)
        else:
            preferred_skills.append(skill)
    if required_skills:
        doc['requiredSkills'] = required_skills
    if preferred_skills:
        doc['preferredSkills'] = preferred_skills
    # location
    location = {}
    for k in ('city', 'province', 'zipcode', 'country'):
        if s_ := doc.pop(k, None):
            location[k] = s_
    if location:
        doc['locations'] = [location]
    return doc
