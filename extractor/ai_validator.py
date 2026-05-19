"""Multi-LLM validator using the centralized router."""
import json
import time
from typing import Dict, Any, List
from llm_router import ROUTER

VALIDATION_PROMPT = """You are a procurement data quality analyst. Validate and correct this extracted quote data.

EXTRACTED DATA (from heuristic parsing):
{chunk}

RAW TEXT EXCERPT (first 2000 chars):
{excerpt}

TASKS:
1. Verify the vendor name (correct it if wrong)
2. Verify the total price is sensible
3. For each service line item: verify name, sku, quantity, unitPrice
4. Identify any missing services from the raw text
5. Flag any suspicious data (e.g. price=0, qty=0, mismatched sku)

DO NOT categorize services here — that's done in a separate step.
Just preserve any existing category/subcategory or use generic placeholders.

RETURN STRICT JSON:
{{
  "vendor": "string",
  "project": "Panasonic|Idemia|Tenneco|Unknown",
  "country": "string",
  "region": "EMEA|APAC|Americas|Global",
  "price_total": number,
  "year": number,
  "quarter": "Q1|Q2|Q3|Q4 or null",
  "quoteDate": "YYYY-MM-DD or null",
  "services": [
    {{"name": "string", "sku": "string", "qty": number, "unitPrice": number}}
  ],
  "validation_notes": ["list of issues found"],
  "confidence": "high|medium|low"
}}
"""


def validate_chunk(chunk: Dict[str, Any]) -> Dict[str, Any]:
    """Validate a single extracted chunk via the LLM router."""
    if chunk.get('extraction_status') == 'failed':
        return chunk
    
    excerpt = chunk.pop('raw_text_excerpt', '')
    payload = {k: v for k, v in chunk.items() if k != 'raw_text_excerpt'}
    
    prompt = VALIDATION_PROMPT.format(
        chunk=json.dumps(payload, indent=2),
        excerpt=excerpt[:2000]
    )
    
    response = ROUTER.call(
        prompt=prompt,
        system='You return strict JSON only. No markdown, no commentary.',
        max_tokens=4000,
        temperature=0.1,
    )
    
    if not response:
        # All LLMs dead → return heuristic data as-is
        chunk['extraction_status'] = 'heuristic_only'
        chunk['validation_notes'] = ['All LLM providers unavailable']
        return chunk
    
    try:
        validated = json.loads(response)
        validated['file'] = chunk.get('file', 'unknown')
        validated['extraction_status'] = 'validated'
        # Preserve heuristic-detected fields if LLM omitted them
        for k in ('category', 'subcat'):
            if k not in validated and k in chunk:
                validated[k] = chunk[k]
        return validated
    except json.JSONDecodeError as e:
        print(f'  ⚠️ Invalid JSON from LLM: {str(e)[:100]}')
        chunk['extraction_status'] = 'validation_failed'
        chunk['validation_notes'] = [f'LLM returned invalid JSON: {str(e)[:100]}']
        return chunk


def validate_batch(chunks: List[Dict]) -> List[Dict]:
    results = []
    for i, chunk in enumerate(chunks):
        # Stop wasting cycles if every provider is dead
        if ROUTER.all_dead():
            print(f'  ⛔ All LLMs dead — keeping remaining {len(chunks) - i} chunks as heuristic-only')
            for c in chunks[i:]:
                c['extraction_status'] = 'heuristic_only'
                c['validation_notes'] = ['All LLM providers unavailable']
                results.append(c)
            break
        
        print(f'  [{i+1}/{len(chunks)}] Validating {chunk.get("file", "?")}')
        validated = validate_chunk(chunk)
        results.append(validated)
        time.sleep(0.25)
    
    return results
