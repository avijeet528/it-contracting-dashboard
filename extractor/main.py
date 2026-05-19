"""
Main orchestrator. Routes to:
  - Process A (mode=ai_only):  LLM extracts everything
  - Process B (mode=hybrid):   Heuristic → JSON chunks → LLM validate → web enrich
"""
import os
import json
import sys
from pathlib import Path
from config import CFG

def find_quote_files(quotes_dir: str = None) -> list:
    """Walk the quotes/ tree and return all parseable files.
    Tries multiple locations so it works locally AND in CI."""
    extensions = {'.pdf', '.xlsx', '.xls', '.docx', '.doc', '.csv', '.txt'}
    
    # Try multiple candidate paths
    candidates = [
        quotes_dir,                          # whatever was passed
        CFG.quotes_dir,                      # from config
        '../quotes',                         # repo root from extractor/
        'quotes',                            # repo root if cwd is root
        os.path.join(os.path.dirname(__file__), '..', 'quotes'),  # absolute
    ]
    
    found_dir = None
    for c in candidates:
        if c and os.path.isdir(c):
            found_dir = c
            break
    
    if not found_dir:
        print(f'❌ Could not find quotes/ directory.')
        print(f'   Tried: {[c for c in candidates if c]}')
        print(f'   CWD:   {os.getcwd()}')
        print(f'   Files in CWD: {os.listdir(".")[:20]}')
        return []
    
    print(f'📁 Using quotes directory: {os.path.abspath(found_dir)}')
    
    files = []
    for root, _, names in os.walk(found_dir):
        for n in names:
            if Path(n).suffix.lower() in extensions:
                files.append(os.path.join(root, n))
    
    return sorted(files)

def run_hybrid_pipeline(files: list) -> list:
    """Process B: heuristic → LLM validate → AI categorize → web enrich"""
    from heuristic_extractor import extract_chunks
    from ai_validator import validate_batch
    from ai_categorizer import categorize_services, apply_categorizations
    from llm_router import ROUTER
    
    print(f'\n📄 Step 1/4: Heuristic extraction ({len(files)} files)')
    chunks = []
    for i, fp in enumerate(files):
        print(f'  [{i+1}/{len(files)}] {os.path.basename(fp)}')
        try:
            chunks.append(extract_chunks(fp))
        except Exception as e:
            print(f'    ❌ Failed: {e}')
            chunks.append({'file': os.path.basename(fp),
                          'extraction_status': 'failed', 'reason': str(e)})
    
    print(f'\n🧠 Step 2/4: LLM validation (auto-failover Groq → Llama → OpenAI)')
    validated = validate_batch(chunks)
    print(f'\n{ROUTER.stats()}')
    
    print(f'\n🏷️  Step 3/4: AI categorization (services → categories/subcategories)')
    cat_lookup = categorize_services(validated)
    validated = apply_categorizations(validated, cat_lookup)
    print(f'  ✅ Categorized {len(cat_lookup)} unique services')
    print(f'\n{ROUTER.stats()}')
    
    print(f'\n🌐 Step 4/4: Web price enrichment (optional)')
    if os.getenv('ENABLE_WEB_ENRICH', 'false').lower() == 'true':
        try:
            from web_scraper import enrich_with_web_prices
            for i, rec in enumerate(validated):
                print(f'  [{i+1}/{len(validated)}] {rec.get("file", "?")}')
                enrich_with_web_prices(rec)
        except ImportError:
            print('  ⚠️ Skipping (duckduckgo-search not installed)')
    else:
        print('  ⏭️  Skipped (set ENABLE_WEB_ENRICH=true to enable)')
    
    return validated

def run_ai_only_pipeline(files: list) -> list:
    """Process A: LLM does it all"""
    from ai_extractor import ai_extract_full
    
    results = []
    for i, fp in enumerate(files):
        print(f'  [{i+1}/{len(files)}] {os.path.basename(fp)}')
        try:
            rec = ai_extract_full(fp)
            if rec:
                # Add folder/project info from path
                parts = Path(fp).parts
                if len(parts) > 1 and parts[-2] != 'quotes':
                    rec['folder'] = parts[-2]
                results.append(rec)
            else:
                print(f'    ⚠️ No data extracted')
        except Exception as e:
            print(f'    ❌ Failed: {e}')
    return results

def normalize_for_dashboard(records: list) -> list:
    """
    Normalize LLM outputs to match the dashboard's expected schema:
    {proj, region, country, cat, subcat, vendor, file, services[{name,sku,qty,unitPrice,webPrice,webRef}], price, year, quarter, quoteDate}
    """
    out = []
    for r in records:
        if not r or r.get('extraction_status') in ('failed', 'validation_failed'):
            continue
        
        normalized = {
            'proj':       r.get('project') or r.get('proj') or 'Unknown',
            'region':     r.get('region', 'Global'),
            'country':    r.get('country', 'Multi-Region'),
            'cat':        r.get('category') or r.get('cat') or 'Cybersecurity',
            'subcat':     r.get('subcat', 'General'),
            'vendor':     r.get('vendor', 'Unknown'),
            'file':       r.get('file', 'unknown.pdf'),
            'folder':     r.get('folder'),
            'services':   r.get('services', []),
            'price':      float(r.get('price_total') or r.get('price') or 0),
            'year':       int(r.get('year') or 2025),
            'quarter':    r.get('quarter'),
            'quoteDate':  r.get('quoteDate'),
        }
        # Drop records where price could not be determined
        if normalized['price'] <= 0 and not normalized['services']:
            continue
        out.append(normalized)
    return out

def main():
    CFG.summary()
    
    files = find_quote_files(CFG.quotes_dir)
    print(f'\n🔎 Found {len(files)} files to process\n')
    
    if not files:
        print('❌ No files found. Add quotes to ./quotes/ and re-run.')
        sys.exit(1)
    
    if CFG.mode == 'ai_only':
        print('🤖 Process A: AI-only extraction')
        records = run_ai_only_pipeline(files)
    else:
        print('🔬 Process B: Hybrid heuristic + AI validation')
        records = run_hybrid_pipeline(files)
    
    # Normalize
    final = normalize_for_dashboard(records)
    
    # Write output
    output_path = CFG.output_file
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final, f, indent=2, ensure_ascii=False)
    
    print(f'\n✅ Wrote {len(final)} records to {output_path}')
    print(f'   Failed/skipped: {len(records) - len(final)}')
    print(f'   Total processed: {len(records)}')

if __name__ == '__main__':
    main()
