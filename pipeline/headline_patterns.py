"""Headline blocklist patterns for filtering out non-ICP contacts."""

import re

HEADLINE_PATTERNS = [
    # Consultants
    (re.compile(r'consultant', re.I), 'consultant'),
    (re.compile(r'consulting', re.I), 'consulting'),
    (re.compile(r'advisory', re.I), 'advisory'),
    (re.compile(r'advisor', re.I), 'advisor'),
    (re.compile(r'adviser', re.I), 'adviser'),
    # Freelancers
    (re.compile(r'freelance', re.I), 'freelance'),
    (re.compile(r'freelancer', re.I), 'freelancer'),
    (re.compile(r'independent', re.I), 'independent'),
    # Agency
    (re.compile(r'agency', re.I), 'agency'),
    (re.compile(r'we help', re.I), 'we help'),
    (re.compile(r'we build', re.I), 'we build'),
    (re.compile(r'we engineer', re.I), 'we engineer'),
    (re.compile(r'we design', re.I), 'we design'),
    (re.compile(r'we create', re.I), 'we create'),
    (re.compile(r'we deliver', re.I), 'we deliver'),
    (re.compile(r'we scale', re.I), 'we scale'),
    (re.compile(r'helping\s+.*?\s*(companies|businesses|brands|startups|teams|agencies)', re.I), 'helping companies/businesses'),
    # Pitch-style / service seller language
    (re.compile(r'for hire', re.I), 'for hire'),
    (re.compile(r'available for', re.I), 'available for'),
    (re.compile(r'open to work', re.I), 'open to work'),
    (re.compile(r'seeking opportunities', re.I), 'seeking opportunities'),
    (re.compile(r'seeking for opportunities', re.I), 'seeking for opportunities'),
    (re.compile(r'open for projects', re.I), 'open for projects'),
    (re.compile(r'open for clients', re.I), 'open for clients'),
    (re.compile(r'\bstrategist\b', re.I), 'strategist'),
    (re.compile(r'grow(ing)? your', re.I), 'grow(ing) your'),
    (re.compile(r'scale your', re.I), 'scale your'),
    (re.compile(r'boost your', re.I), 'boost your'),
    (re.compile(r'your sales', re.I), 'your sales'),
    (re.compile(r'your business', re.I), 'your business'),
    (re.compile(r'your revenue', re.I), 'your revenue'),
    (re.compile(r'your growth', re.I), 'your growth'),
    (re.compile(r'your customers', re.I), 'your customers'),
    (re.compile(r'done for you', re.I), 'done for you'),
    (re.compile(r'done with you', re.I), 'done with you'),
    (re.compile(r'on autopilot', re.I), 'on autopilot'),
    (re.compile(r'book meetings', re.I), 'book meetings'),
    (re.compile(r'book demos', re.I), 'book demos'),
    (re.compile(r'lead gen\b', re.I), 'lead gen'),
    (re.compile(r'lead generation', re.I), 'lead generation'),
    (re.compile(r'your onboarding', re.I), 'your onboarding'),
    (re.compile(r'into revenue', re.I), 'into revenue'),
    (re.compile(r'into conversions', re.I), 'into conversions'),
    (re.compile(r"let's talk", re.I), "let's talk"),
    (re.compile(r'\bi help\b', re.I), 'i help'),
    (re.compile(r'\binbound\b', re.I), 'inbound'),
    (re.compile(r'scaling\s+.*?(companies|businesses|startups|saas)', re.I), 'scaling companies'),
    (re.compile(r'\bcoach\b', re.I), 'coach'),
    (re.compile(r'\bspeaker\b', re.I), 'speaker'),
    (re.compile(r'\bevangelist\b', re.I), 'evangelist'),
    (re.compile(r'\bentrepreneur\b', re.I), 'entrepreneur'),
    (re.compile(r'\bempowering\b', re.I), 'empowering'),
    (re.compile(r'managing partner', re.I), 'managing partner'),
    (re.compile(r'revenue architects?', re.I), 'revenue architect(s)'),
    (re.compile(r'for visionary', re.I), 'for visionary'),
    (re.compile(r'solutions partner', re.I), 'solutions partner'),
    (re.compile(r'partner für', re.I), 'partner für'),
    (re.compile(r'try\s+\S+\.(com|io|co|ai)', re.I), 'try [product URL]'),
    # HubSpot ecosystem
    (re.compile(r'hubspot consultant', re.I), 'hubspot consultant'),
    (re.compile(r'hubspot\s+.*?partner', re.I), 'hubspot partner'),
    (re.compile(r'hubspot coach', re.I), 'hubspot coach'),
    (re.compile(r'hubspot certified', re.I), 'hubspot certified'),
    (re.compile(r'hubspot expert', re.I), 'hubspot expert'),
    (re.compile(r'hubspot implementation', re.I), 'hubspot implementation'),
    (re.compile(r'hubspot app', re.I), 'hubspot app'),
    (re.compile(r'build(ing)? hubspot', re.I), 'build(ing) hubspot'),
    (re.compile(r'revops agency', re.I), 'revops agency'),
    (re.compile(r'revops fixer', re.I), 'revops fixer'),
    (re.compile(r'martech agency', re.I), 'martech agency'),
    (re.compile(r'crm consultant', re.I), 'crm consultant'),
    (re.compile(r'crm implementation', re.I), 'crm implementation'),
]


def check_headline(headline):
    """Returns matched keyword if headline matches a blocklist pattern, else None."""
    if not headline:
        return None
    for pattern, keyword in HEADLINE_PATTERNS:
        if pattern.search(headline):
            return keyword
    return None
