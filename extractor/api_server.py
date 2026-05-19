"""
Lightweight chatbot endpoint that loads catalog_data.json and grounds answers in it.
Run with: uvicorn api_server:app --port 8000
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import json
import os
from pathlib import Path

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
)

# Load catalog at startup
CATALOG_PATH = Path(__file__).parent.parent / 'catalog_data.json'
CATALOG = []
if CATALOG_PATH.exists():
    with open(CATALOG_PATH) as f:
        CATALOG = json.load(f)
    print(f'📚 Loaded {len(CATALOG)} catalog records')

class ChatRequest(BaseModel):
    question: str
    history: list = []

def build_catalog_summary() -> str:
    """Compress the catalog into a structured summary for the LLM."""
    if not CATALOG:
        return 'No catalog data available.'
    
    from collections import Counter, defaultdict
    
    vendors = Counter(r['vendor'] for r in CATALOG)
    cats = Counter(r['cat'] for r in CATALOG)
    projects = Counter(r['proj'] for r in CATALOG)
    
    # Service-level rollup
    svc_stats = defaultdict(lambda: {'unitPrices': [], 'vendors': set(), 'count': 0})
    for r in CATALOG:
        for s in r.get('services', []):
            if isinstance(s, dict):
                key = s.get('name', '')
                if key:
                    svc_stats[key]['count'] += 1
                    svc_stats[key]['vendors'].add(r['vendor'])
                    if s.get('unitPrice', 0) > 0:
                        svc_stats[key]['unitPrices'].append(s['unitPrice'])
    
    # Top services by frequency
    top_services = sorted(svc_stats.items(), key=lambda x: -x[1]['count'])[:30]
    
    summary = [
        f'CATALOG: {len(CATALOG)} quote records',
        f'TOTAL SPEND: ${sum(r.get("price",0) for r in CATALOG):,.0f}',
        f'VENDORS: ' + ', '.join(f'{v} ({c})' for v, c in vendors.most_common(10)),
        f'CATEGORIES: ' + ', '.join(f'{c} ({n})' for c, n in cats.most_common()),
        f'PROJECTS: ' + ', '.join(f'{p} ({n})' for p, n in projects.most_common()),
        '',
        'TOP SERVICES (with avg unit price):'
    ]
    for name, stats in top_services:
        if stats['unitPrices']:
            avg = sum(stats['unitPrices']) / len(stats['unitPrices'])
            mn = min(stats['unitPrices'])
            mx = max(stats['unitPrices'])
            summary.append(
                f'  • {name}: ${mn:,.2f}–${mx:,.2f} (avg ${avg:,.2f}), '
                f'{stats["count"]} quotes, {len(stats["vendors"])} vendors'
            )
    
    return '\n'.join(summary)

CATALOG_SUMMARY = build_catalog_summary()

CHAT_SYSTEM_PROMPT = f"""You are an expert IT-procurement analyst helping a user navigate their vendor catalog.

You have access to this catalog data (compressed summary):

{CATALOG_SUMMARY}

When answering:
- Quote specific vendor names, prices, and SKUs from the catalog when relevant
- Give actionable advice (which vendor to choose, where to negotiate)
- Use markdown formatting: **bold** for emphasis, bullet lists, tables when comparing
- If the question can't be answered from the catalog, say so clearly
- Be concise — aim for 3-5 short paragraphs max
- Always tie your answer back to the actual data
"""

@app.get('/api/health')
def health():
    return {'status': 'ok', 'records': len(CATALOG)}

@app.post('/api/chat')
def chat(req: ChatRequest):
    """Catalog-aware chatbot. Tries Groq (fast), falls back to OpenAI."""
    messages = [{'role': 'system', 'content': CHAT_SYSTEM_PROMPT}]
    for h in req.history[-6:]:  # last 3 turns
        messages.append(h)
    messages.append({'role': 'user', 'content': req.question})
    
    # Try Groq first (fastest + free)
    if os.getenv('GROQ_API_KEY'):
        try:
            from groq import Groq
            client = Groq(api_key=os.getenv('GROQ_API_KEY'))
            r = client.chat.completions.create(
                model='llama-3.3-70b-versatile',
                messages=messages,
                temperature=0.4,
                max_tokens=800,
            )
            return {'answer': r.choices[0].message.content, 'engine': 'groq'}
        except Exception as e:
            print(f'Groq failed: {e}')
    
    # Fallback to OpenAI
    if os.getenv('OPENAI_API_KEY'):
        try:
            from openai import OpenAI
            client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
            r = client.chat.completions.create(
                model='gpt-4o-mini',
                messages=messages,
                temperature=0.4,
                max_tokens=800,
            )
            return {'answer': r.choices[0].message.content, 'engine': 'openai'}
        except Exception as e:
            print(f'OpenAI failed: {e}')
    
    raise HTTPException(503, 'No LLM available')
