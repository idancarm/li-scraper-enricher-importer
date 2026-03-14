"""AI-powered ICP review of contacts using Claude."""

from __future__ import annotations

import json

import anthropic

from .config import ICP_REVIEW_MODEL, ICP_REVIEW_BATCH_SIZE
from .supabase_client import get_contacts_by_status, update_contact_status

SYSTEM_PROMPT = """\
You are an ICP (Ideal Customer Profile) evaluator. Given the ICP criteria below \
and a list of LinkedIn contacts, decide for each person whether they match the \
ICP and should be enriched for sales outreach.

## ICP Criteria
{icp_config}

## Instructions
- For each person, return "enrich" if they are a potential ICP match, "skip" if not
- Base your decision on their headline, company, and job title
- Provide a brief reason (max 10 words)
- When uncertain, lean toward "enrich"
- Return ONLY a valid JSON array, no other text
- Format: [{{"index": 0, "recommendation": "enrich", "reason": "VP Sales at mid-market SaaS"}}, ...]
"""


def _call_claude(api_key: str, system: str, user_msg: str) -> list[dict]:
    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model=ICP_REVIEW_MODEL,
        max_tokens=4096,
        system=system,
        messages=[{"role": "user", "content": user_msg}],
    )
    text = response.content[0].text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    return json.loads(text)


def review_contacts(actor: dict, run_id: str, icp_config: str) -> dict:
    """Review filtered contacts against ICP criteria using Claude AI.

    Reads contacts with status='filtered', sets icp_recommendation and icp_reason.
    Contacts recommended 'skip' are updated to status='excluded' with reason 'icp_review'.
    Returns summary dict.
    """
    api_key = actor.get("anthropic_api_key", "")
    if not api_key:
        raise RuntimeError(f"Actor '{actor.get('name', '?')}' has no anthropic_api_key set")

    contacts = get_contacts_by_status(actor["id"], "filtered", pipeline_run_id=run_id)
    if not contacts:
        print("  No filtered contacts to review.")
        return {"total": 0, "enrich": 0, "skip": 0}

    print(f"  Reviewing {len(contacts)} contacts against ICP...")

    system = SYSTEM_PROMPT.format(icp_config=icp_config)
    enrich_count = 0
    skip_count = 0

    for batch_start in range(0, len(contacts), ICP_REVIEW_BATCH_SIZE):
        batch = contacts[batch_start:batch_start + ICP_REVIEW_BATCH_SIZE]
        batch_data = []
        for i, c in enumerate(batch):
            batch_data.append({
                "index": i,
                "name": f"{c.get('first_name', '')} {c.get('last_name', '')}".strip(),
                "headline": c.get("headline", ""),
                "company": c.get("company", ""),
                "jobtitle": c.get("jobtitle", ""),
            })

        try:
            results = _call_claude(api_key, system, json.dumps(batch_data))
            result_map = {r["index"]: r for r in results}
        except Exception as e:
            print(f"    [icp_review] API error on batch starting at row {batch_start}: {e}")
            result_map = {}

        for i, contact in enumerate(batch):
            r = result_map.get(i, {})
            recommendation = r.get("recommendation", "enrich")
            reason = r.get("reason", "")

            if recommendation == "skip":
                update_contact_status(
                    contact["id"], "excluded",
                    exclusion_reason="icp_review",
                    icp_recommendation="skip",
                    icp_reason=reason,
                )
                skip_count += 1
            else:
                update_contact_status(
                    contact["id"], "filtered",
                    icp_recommendation="enrich",
                    icp_reason=reason,
                )
                enrich_count += 1

    summary = {"total": len(contacts), "enrich": enrich_count, "skip": skip_count}
    print(f"  ICP review done: {enrich_count} enrich, {skip_count} skip")
    return summary
