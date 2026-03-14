"""CSV column definitions and write helper for pipeline exports."""

import csv

CSV_COLUMNS = [
    "First Name", "Last Name", "Email", "Company", "Job Title",
    "Headline", "LinkedIn URL", "Status", "Enriched By",
]


def write_csv(path, contacts: list[dict]):
    """Write contacts to a CSV file."""
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for c in contacts:
            writer.writerow({
                "First Name": c.get("first_name", ""),
                "Last Name": c.get("last_name", ""),
                "Email": c.get("email", ""),
                "Company": c.get("company", ""),
                "Job Title": c.get("jobtitle", ""),
                "Headline": c.get("headline", ""),
                "LinkedIn URL": c.get("linkedin_url", ""),
                "Status": c.get("status", ""),
                "Enriched By": c.get("enriched_by", ""),
            })
