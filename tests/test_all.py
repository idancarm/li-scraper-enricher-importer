"""Tests for the li-scraper pipeline."""

import pytest

from pipeline.headline_patterns import check_headline
from pipeline.enrich import _is_personal_email, _parse_headline


# --- Headline filter tests ---

class TestHeadlineFilter:
    def test_blocks_consultants(self):
        assert check_headline("Marketing Consultant at Acme") == "consultant"
        assert check_headline("Consulting Director") == "consulting"
        assert check_headline("Senior Advisory Board Member") == "advisory"

    def test_blocks_freelancers(self):
        assert check_headline("Freelance Designer") == "freelance"
        assert check_headline("Independent Contractor") == "independent"

    def test_blocks_agency(self):
        assert check_headline("CEO at Digital Agency") == "agency"
        assert check_headline("We help companies grow") == "we help"
        assert check_headline("We build amazing products") == "we build"

    def test_blocks_pitch_language(self):
        assert check_headline("Growing your revenue 10x") is not None
        assert check_headline("Scale your business fast") is not None
        assert check_headline("Book meetings on autopilot") is not None
        assert check_headline("Lead generation expert") is not None
        assert check_headline("Let's talk about your growth") is not None
        assert check_headline("I help founders scale") is not None

    def test_blocks_hubspot_ecosystem(self):
        assert check_headline("HubSpot Consultant") is not None
        assert check_headline("Certified HubSpot Partner") is not None
        assert check_headline("HubSpot Implementation Specialist") is not None
        assert check_headline("Building HubSpot integrations") is not None
        assert check_headline("RevOps Agency Owner") is not None
        assert check_headline("CRM Consultant") is not None

    def test_blocks_misc(self):
        assert check_headline("Keynote Speaker & Author") is not None
        assert check_headline("Sales Coach") is not None
        assert check_headline("Tech Evangelist") is not None
        assert check_headline("Serial Entrepreneur") is not None
        assert check_headline("Open to work") is not None
        assert check_headline("Available for freelance projects") is not None
        assert check_headline("Strategist at Company") is not None

    def test_passes_normal_titles(self):
        assert check_headline("VP of Sales at Salesforce") is None
        assert check_headline("Head of Marketing") is None
        assert check_headline("CTO at TechCorp") is None
        assert check_headline("Software Engineer") is None
        assert check_headline("Product Manager at Google") is None
        assert check_headline("Director of Operations") is None
        assert check_headline("Chief Revenue Officer") is None
        assert check_headline("Senior Account Executive") is None

    def test_empty(self):
        assert check_headline("") is None
        assert check_headline(None) is None


# --- Personal email detection ---

class TestPersonalEmail:
    def test_personal_emails(self):
        assert _is_personal_email("john@gmail.com") is True
        assert _is_personal_email("jane@yahoo.com") is True
        assert _is_personal_email("bob@hotmail.com") is True
        assert _is_personal_email("alice@outlook.com") is True
        assert _is_personal_email("test@icloud.com") is True

    def test_business_emails(self):
        assert _is_personal_email("john@acme.com") is False
        assert _is_personal_email("jane@company.io") is False
        assert _is_personal_email("bob@startup.co") is False


# --- Headline parser ---

class TestParseHeadline:
    def test_at_pattern(self):
        assert _parse_headline("VP of Sales at Salesforce") == {
            "jobtitle": "VP of Sales", "company": "Salesforce"
        }

    def test_pipe_separator(self):
        assert _parse_headline("Software Engineer | Google") == {
            "jobtitle": "Software Engineer", "company": "Google"
        }

    def test_dash_separator(self):
        assert _parse_headline("Product Manager - Meta") == {
            "jobtitle": "Product Manager", "company": "Meta"
        }

    def test_emdash_separator(self):
        assert _parse_headline("CTO \u2014 TechCorp") == {
            "jobtitle": "CTO", "company": "TechCorp"
        }

    def test_no_separator(self):
        assert _parse_headline("Head of Marketing") == {
            "jobtitle": "Head of Marketing", "company": ""
        }

    def test_empty(self):
        assert _parse_headline("") == {"jobtitle": "", "company": ""}
        assert _parse_headline(None) == {"jobtitle": "", "company": ""}
