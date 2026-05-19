"""
Multi-LLM validator. Takes heuristic chunks and:
  1. Validates each field
  2. Fills in missing data
  3. Cross-references with web prices (where available)

Uses fallback chain: OpenAI → Groq → Llama
"""
import json
import time
from typing import Dict, Any, Optional, List
from config import CFG

VALIDATION_PROMPT = """You are a procurement data quality analyst. Validate and correct this extracted quote data.

EXTRACTED DATA (from heuristic parsing):
{chunk}

RAW TEXT EXCERPT (first 2000 chars):
{excerpt}

TASKS:
1. Verify the vendor name (correct it if wrong)
2. Verify the total price is sensible
3. Verify category matches the services
4. For each service line item: verify name, sku, quantity, unitPrice make sense
5. Identify any missing services from the raw text
6. Flag any suspicious data (e.g. price=0, qty=0, mismatched sku)

RETURN STRICT JSON in this exact schema (no markdown, no explanation):
{{
  "vendor": "string",
  "project": "Panasonic|Idemia|Tenneco|Unknown",
  "category": "Cybersecurity|Network & Telecom|Hosting|M365 & Power Platform|Service Management (SNow)|IdAM",
  "subcat": "string",
  "country": "string",
  "region": "EMEA|APAC|Americas|Global",
  "price_total": number,
  "year": number,
  "quarter": "Q1|Q2|Q3|Q4|null",
  "quoteDate": "YYYY-MM-DD or null",
  "services": [
    {{"name": "string", "sku": "string", "qty": number, "unitPrice": number}}
  ],
  "validation_notes": ["list of issues found"],
  "confidence": "high|medium|low"
}}
"""

def call_openai(prompt: str, model: str = None) -> Optional[str]:
    if not CFG.has_openai(): return None
    try:
        from openai import OpenAI
        client = OpenAI(api_key=CFG.openai_key)
        response = client.chat.completions.create(
            model=model or CFG.openai_model,
            messages=[
                {'role': 'system', 'content': 'You return strict JSON only. No markdown, no commentary.'},
                {'role': 'user', 'content': prompt}
            ],
            response_format={'type': 'json_object'},
            temperature=0.1,
            max_tokens=4000,
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f'  ⚠️ OpenAI failed: {e}')
        return None

def call_groq(prompt: str) -> Optional[str]:
    if not CFG.has_groq(): return None
    try:
        from groq import Groq
        client = Groq(api_key=CFG.groq_key)
        response = client.chat.completions.create(
            model=CFG.groq_model,
            messages=[
                {'role': 'system', 'content': 'You return strict JSON only. No markdown, no commentary.'},
                {'role': 'user', 'content': prompt}
            ],
            temperature=0.1,
            max_tokens=4000,
            response_format={'type': 'json_object'},
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f'  ⚠️ Groq failed: {e}')
        return None

def call_llama(prompt: str) -> Optional[str]:
    """LlamaParse focuses on parsing; for chat we'd need llama-api separately.
    If you have a Llama chat endpoint, plug it in here."""
    return None

def validate_chunk(chunk: Dict[str, Any]) -> Dict[str, Any]:
    """Run validation through fallback chain."""
    if chunk.get('extraction_status') == 'failed':
        return chunk
    
    # Build prompt
    excerpt = chunk.pop('raw_text_excerpt', '')
    prompt = VALIDATION_PROMPT.format(
        chunk=json.dumps({k: v for k, v in chunk.items() if k != 'raw_text_excerpt'}, indent=2),
        excerpt=excerpt[:2000]
    )
    
    response_text = None
    used_llm = None
    
    # Try OpenAI first (best quality)
    if not response_text:
        response_text = call_openai(prompt)
        if response_text: used_llm = 'openai'
    
    # Fallback to Groq (fast + free tier)
    if not response_text:
        response_text = call_groq(prompt)
        if response_text: used_llm = 'groq'
    
    # No LLM available → return heuristic-only
    if not response_text:
        chunk['extraction_status'] = 'heuristic_only'
        chunk['validation_notes'] = ['No LLM available for validation']
        return chunk
    
    # Parse LLM response
    try:
        validated = json.loads(response_text)
        validated['file'] = chunk['file']
        validated['extraction_status'] = 'validated'
        validated['validated_by'] = used_llm
        return validated
    except json.JSONDecodeError as e:
        print(f'  ⚠️ Invalid JSON from {used_llm}: {e}')
        chunk['extraction_status'] = 'validation_failed'
        chunk['validation_notes'] = [f'LLM returned invalid JSON: {str(e)[:100]}']
        return chunk

def validate_batch(chunks: List[Dict]) -> List[Dict]:
    results = []
    for i, chunk in enumerate(chunks):
        print(f'  [{i+1}/{len(chunks)}] Validating {chunk.get("file", "?")}...')
        validated = validate_chunk(chunk)
        results.append(validated)
        time.sleep(0.3)  # rate limit respect
    return results
