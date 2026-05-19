"""
AI-powered service categorizer. Takes the raw services from extracted records
and uses an LLM to assign accurate (category, subcategory) labels based on
SKU patterns + service names + (optionally) web search.

This runs AFTER extraction so we can categorize all services in batch.
"""
import json
import time
from typing import Dict, List, Set, Tuple
from llm_router import ROUTER

# Master taxonomy — these are the canonical buckets shown in the dashboard.
# The AI is allowed to add new subcats but must fit one of these top categories.
TAXONOMY = {
    'Network & Telecom': {
        'desc': 'Routers, switches, wireless APs, network licensing, telecom circuits',
        'subcats': [
            'Routing & Switching', 'Wireless / WiFi', 'SD-WAN',
            'Cisco DNA Licensing', 'Network Services', 'Cabling & Optics',
            'Firewalls', 'Network Monitoring'
        ]
    },
    'Cybersecurity': {
        'desc': 'Endpoint protection, threat detection, IAM, security awareness',
        'subcats': [
            'Endpoint Protection', 'EDR/XDR', 'Email Security',
            'Identity & Access', 'Security Awareness', 'Vulnerability Mgmt',
            'Threat Intelligence', 'CASB / Cloud Security', 'Zero Trust'
        ]
    },
    'M365 & Power Platform': {
        'desc': 'Microsoft 365 licensing, Copilot, Power BI, Teams, Visio',
        'subcats': [
            'M365 E3/E5 Licenses', 'M365 F-series (Frontline)',
            'Copilot', 'Power Platform', 'Visio / Project',
            'Teams Phone', 'Defender for M365', 'Windows 365 Cloud PC',
            'Exchange Online', 'Azure Commitments'
        ]
    },
    'Hosting': {
        'desc': 'Servers, storage, virtualization, colocation, databases',
        'subcats': [
            'Compute / Servers', 'Storage', 'Virtualization (VMware)',
            'Database (Oracle/SQL)', 'Colocation', 'Backup & DR',
            'Cloud IaaS', 'HCI'
        ]
    },
    'Service Management (SNow)': {
        'desc': 'ServiceNow ITSM, ITOM, ITBM modules',
        'subcats': [
            'ITSM Core', 'ITOM', 'ITBM', 'HRSD', 'CSM', 'SecOps'
        ]
    },
    'IdAM': {
        'desc': 'Identity migration, Active Directory, IGA tools',
        'subcats': [
            'AD Migration', 'IGA', 'PAM', 'SSO/MFA', 'Directory Services'
        ]
    },
    'Professional Services': {
        'desc': 'Consulting, implementation, support contracts',
        'subcats': [
            'Implementation', 'Consulting', 'Support & Maintenance',
            'Training', 'Managed Services', 'Logistics'
        ]
    }
}

# Build a hint string of common SKU patterns for the LLM
SKU_HINTS = """
COMMON SKU PATTERNS:
  C8300-*, C9300-*, C9200-*, C9800-*  → Cisco Catalyst routers/switches → Network & Telecom / Routing & Switching
  CW9*, MR4*, MR5*                    → Cisco/Meraki Wireless APs       → Network & Telecom / Wireless / WiFi
  DNA-C-*, L-DNA-*, DNA-C8*           → Cisco DNA Licensing            → Network & Telecom / Cisco DNA Licensing
  LIC-MR-*, LIC-C9300-*               → Meraki Licenses                 → Network & Telecom / Cisco DNA Licensing
  CON-*, SVS-*                        → Cisco Support Contracts         → Professional Services / Support & Maintenance
  STACK-T1-*, PWR-CORD-*, GLC-*, SFP-*→ Cabling/Optics                  → Network & Telecom / Cabling & Optics
  NCE*-*                              → Microsoft NCE SKU              → M365 & Power Platform (depends on product)
  AAD-*, HWN-*, N9U-*, I76-*, TQA-*   → Microsoft Volume Licensing     → M365 & Power Platform
  Q4B-*, NM1-*                        → Microsoft Defender             → Cybersecurity / Threat Intelligence
"""


CATEGORIZE_PROMPT = """You are an IT-procurement taxonomist. Categorize each service below.

ALLOWED TOP CATEGORIES (you MUST pick one of these):
{categories}

{sku_hints}

INSTRUCTIONS:
- For each service, return: category (from list above), subcategory (be specific — invent a clean name if needed), and confidence
- Use the SKU pattern as the strongest signal
- If unsure, use the service name semantics
- Subcategory should be human-readable (e.g. "Wireless / WiFi" not "WIRELESS_AP_LIC")
- Group related items consistently — e.g. all C8300 routers should land in the same subcat

SERVICES TO CATEGORIZE:
{services}

Return STRICT JSON:
{{
  "categorizations": [
    {{
      "name": "exact service name as given",
      "sku": "exact sku as given",
      "category": "one of the allowed top categories",
      "subcategory": "specific subcategory",
      "vendor_hint": "manufacturer brand if identifiable (e.g. Cisco, Microsoft, Meraki)",
      "confidence": "high|medium|low"
    }}
  ]
}}
"""


def _build_categories_block() -> str:
    lines = []
    for cat, info in TAXONOMY.items():
        subs = ', '.join(info['subcats'])
        lines.append(f'  • {cat}: {info["desc"]}\n    suggested subcats: {subs}')
    return '\n'.join(lines)


def _chunked(lst: List, size: int):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def categorize_services(records: List[Dict], batch_size: int = 25) -> Dict[Tuple[str, str], Dict]:
    """
    Collect all unique services across records, send to LLM in batches,
    return a lookup: (name, sku) -> {category, subcategory, vendor_hint, confidence}
    """
    # Step 1: collect unique (name, sku) pairs
    unique: Dict[Tuple[str, str], Dict] = {}
    for rec in records:
        for s in rec.get('services', []):
            if not isinstance(s, dict):
                continue
            key = (s.get('name', '').strip(), (s.get('sku') or '').strip())
            if key[0] and key not in unique:
                unique[key] = {'name': key[0], 'sku': key[1]}
    
    services = list(unique.values())
    print(f'  📦 Found {len(services)} unique services to categorize')
    
    if not services:
        return {}
    
    # Step 2: process in batches (LLM context is finite)
    cat_block = _build_categories_block()
    results: Dict[Tuple[str, str], Dict] = {}
    
    batches = list(_chunked(services, batch_size))
    for i, batch in enumerate(batches):
        print(f'  🤖 Categorizing batch {i+1}/{len(batches)} ({len(batch)} services)...')
        
        prompt = CATEGORIZE_PROMPT.format(
            categories=cat_block,
            sku_hints=SKU_HINTS,
            services=json.dumps(batch, indent=2)
        )
        
        response = ROUTER.call(
            prompt=prompt,
            system='You are an IT-procurement taxonomist. Return strict JSON only.',
            max_tokens=4000,
            temperature=0.1,
        )
        
        if not response:
            print(f'    ❌ All LLMs failed for this batch — using fallback rules')
            for svc in batch:
                key = (svc['name'], svc['sku'])
                results[key] = _fallback_categorize(svc)
            continue
        
        try:
            parsed = json.loads(response)
            cats = parsed.get('categorizations', [])
            for c in cats:
                name = c.get('name', '').strip()
                sku = (c.get('sku') or '').strip()
                category = c.get('category', '').strip()
                
                # Validate category against taxonomy; coerce if invalid
                if category not in TAXONOMY:
                    category = _closest_category(category)
                
                results[(name, sku)] = {
                    'category': category,
                    'subcategory': c.get('subcategory', 'General').strip()[:50],
                    'vendor_hint': c.get('vendor_hint', '').strip()[:50],
                    'confidence': c.get('confidence', 'medium'),
                }
            
            # Fill in any services the LLM skipped
            covered = {(c.get('name', '').strip(), (c.get('sku') or '').strip())
                       for c in cats}
            for svc in batch:
                key = (svc['name'], svc['sku'])
                if key not in covered:
                    results[key] = _fallback_categorize(svc)
        
        except json.JSONDecodeError as e:
            print(f'    ⚠️ JSON parse failed: {e}')
            for svc in batch:
                key = (svc['name'], svc['sku'])
                results[key] = _fallback_categorize(svc)
        
        time.sleep(0.4)  # be kind to APIs
    
    return results


def _closest_category(maybe_cat: str) -> str:
    """If the LLM returned a slightly off label, snap to the nearest valid one."""
    if not maybe_cat:
        return 'Network & Telecom'
    lower = maybe_cat.lower()
    for cat in TAXONOMY:
        if cat.lower() == lower:
            return cat
    # Fuzzy contains
    for cat in TAXONOMY:
        if cat.lower() in lower or lower in cat.lower():
            return cat
    # Token overlap
    tokens = set(lower.split())
    best, best_score = 'Network & Telecom', 0
    for cat in TAXONOMY:
        cat_tokens = set(cat.lower().split())
        score = len(tokens & cat_tokens)
        if score > best_score:
            best, best_score = cat, score
    return best


def _fallback_categorize(svc: Dict) -> Dict:
    """Rule-based fallback if LLM is unavailable."""
    name = svc.get('name', '').lower()
    sku = svc.get('sku', '').lower()
    blob = name + ' ' + sku
    
    rules = [
        ('Cybersecurity', 'Endpoint Protection',
         ['defender', 'crowdstrike', 'sentinelone', 'endpoint']),
        ('Cybersecurity', 'Threat Intelligence',
         ['threat intel', 'q4b-', 'nm1-']),
        ('Cybersecurity', 'Identity & Access',
         ['cyberark', 'okta', 'permission']),
        ('M365 & Power Platform', 'M365 E3/E5 Licenses',
         ['m365 e3', 'm365 e5', 'office 365 e3', 'office 365 e5', 'aad-33168']),
        ('M365 & Power Platform', 'Copilot',
         ['copilot']),
        ('M365 & Power Platform', 'Power Platform',
         ['power apps', 'power bi', 'power automate']),
        ('M365 & Power Platform', 'Visio / Project',
         ['visio', 'project']),
        ('M365 & Power Platform', 'Windows 365 Cloud PC',
         ['w365', 'windows 365', 'cloud pc']),
        ('M365 & Power Platform', 'Teams Phone',
         ['teams phone', 'teams essentials']),
        ('M365 & Power Platform', 'Azure Commitments',
         ['azure consumption', 'az consumption']),
        ('Network & Telecom', 'Wireless / WiFi',
         ['wireless', 'wifi', 'wi-fi', 'cw917', 'mr46', 'meraki mr', 'access point']),
        ('Network & Telecom', 'Cisco DNA Licensing',
         ['dna-c-', 'l-dna', 'dna advantage', 'dna-c8', 'lic-mr', 'lic-c9300']),
        ('Network & Telecom', 'Routing & Switching',
         ['c8300', 'c9300', 'c9200', 'catalyst', 'router', 'switch']),
        ('Network & Telecom', 'Cabling & Optics',
         ['stack-t1', 'sfp-', 'glc-', 'pwr-cord', 'cable', 'optic']),
        ('Professional Services', 'Support & Maintenance',
         ['cx level', 'con-', 'svs-', 'maintenance', 'support enhanced',
          'success track', '24/7', 'on-site']),
        ('Professional Services', 'Logistics',
         ['logistics', 'outbound', 'shipping']),
    ]
    
    for cat, subcat, kws in rules:
        if any(kw in blob for kw in kws):
            return {
                'category': cat,
                'subcategory': subcat,
                'vendor_hint': '',
                'confidence': 'low',
            }
    
    return {
        'category': 'Network & Telecom',
        'subcategory': 'General',
        'vendor_hint': '',
        'confidence': 'low',
    }


def apply_categorizations(records: List[Dict], categorizations: Dict[Tuple[str, str], Dict]) -> List[Dict]:
    """
    Mutate records in-place: each service gets `category` and `subcategory` fields.
    Also recompute the record-level cat/subcat using the dominant category among its services.
    """
    from collections import Counter
    
    for rec in records:
        cats_in_rec = []
        subcats_in_rec = []
        
        for s in rec.get('services', []):
            if not isinstance(s, dict):
                continue
            key = (s.get('name', '').strip(), (s.get('sku') or '').strip())
            cat_info = categorizations.get(key)
            if cat_info:
                s['category'] = cat_info['category']
                s['subcategory'] = cat_info['subcategory']
                if cat_info.get('vendor_hint'):
                    s['vendor_hint'] = cat_info['vendor_hint']
                cats_in_rec.append(cat_info['category'])
                subcats_in_rec.append(cat_info['subcategory'])
        
        # Update the record-level cat/subcat to the most common one
        if cats_in_rec:
            rec['cat'] = Counter(cats_in_rec).most_common(1)[0][0]
        if subcats_in_rec:
            rec['subcat'] = Counter(subcats_in_rec).most_common(1)[0][0]
    
    return records
