#!/usr/bin/env python3
"""Tests for the li-scraper Python pipeline."""

import json
import os
import tempfile
from pathlib import Path

# --- lib.py tests ---

from lib import read_json, write_json


def test_read_json_missing_file():
    result = read_json('/tmp/nonexistent_test_file_xxxxx.json')
    assert result == [], f'Expected empty list, got {result}'


def test_read_write_json_roundtrip():
    with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
        path = f.name
    try:
        data = [{'name': 'Alice'}, {'name': 'Bob'}]
        write_json(path, data)
        result = read_json(path)
        assert result == data, f'Roundtrip failed: {result}'
    finally:
        os.unlink(path)


# --- filter_contacts.py tests ---

from filter_contacts import check_headline


def test_headline_filter_blocks_consultants():
    assert check_headline('Marketing Consultant at Acme') == 'consultant'
    assert check_headline('Consulting Director') == 'consulting'
    assert check_headline('Senior Advisory Board Member') == 'advisory'


def test_headline_filter_blocks_freelancers():
    assert check_headline('Freelance Designer') == 'freelance'
    assert check_headline('Independent Contractor') == 'independent'


def test_headline_filter_blocks_agency():
    assert check_headline('CEO at Digital Agency') == 'agency'
    assert check_headline('We help companies grow') == 'we help'
    assert check_headline('We build amazing products') == 'we build'


def test_headline_filter_blocks_pitch_language():
    assert check_headline('Growing your revenue 10x') is not None
    assert check_headline('Scale your business fast') is not None
    assert check_headline('Book meetings on autopilot') is not None
    assert check_headline('Lead generation expert') is not None
    assert check_headline("Let's talk about your growth") is not None
    assert check_headline('I help founders scale') is not None


def test_headline_filter_blocks_hubspot_ecosystem():
    # These match earlier generic patterns first — still correctly excluded
    assert check_headline('HubSpot Consultant') is not None
    assert check_headline('Certified HubSpot Partner') is not None
    assert check_headline('HubSpot Implementation Specialist') is not None
    assert check_headline('Building HubSpot integrations') is not None
    assert check_headline('RevOps Agency Owner') is not None
    assert check_headline('CRM Consultant') is not None


def test_headline_filter_blocks_misc():
    assert check_headline('Keynote Speaker & Author') is not None
    assert check_headline('Sales Coach') is not None
    assert check_headline('Tech Evangelist') is not None
    assert check_headline('Serial Entrepreneur') is not None
    assert check_headline('Open to work') is not None
    assert check_headline('Available for freelance projects') is not None
    assert check_headline('Strategist at Company') is not None


def test_headline_filter_passes_normal_titles():
    assert check_headline('VP of Sales at Salesforce') is None
    assert check_headline('Head of Marketing') is None
    assert check_headline('CTO at TechCorp') is None
    assert check_headline('Software Engineer') is None
    assert check_headline('Product Manager at Google') is None
    assert check_headline('Director of Operations') is None
    assert check_headline('Chief Revenue Officer') is None
    assert check_headline('Senior Account Executive') is None


def test_headline_filter_empty():
    assert check_headline('') is None
    assert check_headline(None) is None


# --- enrich_contacts.py tests ---

from enrich_contacts import is_personal_email, parse_headline


def test_personal_email_detection():
    assert is_personal_email('john@gmail.com') is True
    assert is_personal_email('jane@yahoo.com') is True
    assert is_personal_email('bob@hotmail.com') is True
    assert is_personal_email('alice@outlook.com') is True
    assert is_personal_email('test@icloud.com') is True


def test_business_email_detection():
    assert is_personal_email('john@acme.com') is False
    assert is_personal_email('jane@company.io') is False
    assert is_personal_email('bob@startup.co') is False


def test_parse_headline_at_pattern():
    result = parse_headline('VP of Sales at Salesforce')
    assert result == {'jobtitle': 'VP of Sales', 'company': 'Salesforce'}


def test_parse_headline_separator_pipe():
    result = parse_headline('Software Engineer | Google')
    assert result == {'jobtitle': 'Software Engineer', 'company': 'Google'}


def test_parse_headline_separator_dash():
    result = parse_headline('Product Manager - Meta')
    assert result == {'jobtitle': 'Product Manager', 'company': 'Meta'}


def test_parse_headline_separator_emdash():
    result = parse_headline('CTO \u2014 TechCorp')
    assert result == {'jobtitle': 'CTO', 'company': 'TechCorp'}


def test_parse_headline_no_separator():
    result = parse_headline('Head of Marketing')
    assert result == {'jobtitle': 'Head of Marketing', 'company': ''}


def test_parse_headline_empty():
    result = parse_headline('')
    assert result == {'jobtitle': '', 'company': ''}

    result = parse_headline(None)
    assert result == {'jobtitle': '', 'company': ''}


# --- Run all tests ---

if __name__ == '__main__':
    test_funcs = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    passed = 0
    failed = 0
    for func in test_funcs:
        try:
            func()
            passed += 1
            print(f'  PASS  {func.__name__}')
        except Exception as e:
            failed += 1
            print(f'  FAIL  {func.__name__}: {e}')

    print(f'\n{passed} passed, {failed} failed, {passed + failed} total')
    if failed:
        raise SystemExit(1)
