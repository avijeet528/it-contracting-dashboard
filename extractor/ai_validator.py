"""
Heuristic extractor for IT contracting quotes.
Extracts: vendor, services, price, country/region, year/quarter, category,
          AND line items (SKU, qty, unit_price, line_total).
"""
import re
from typing import Dict, List, Tuple
from collections import Counter


# ============================================================================
# KNOWN ENTITIES — extend these lists as you onboard more vendors/services
# ============================================================================

KNOWN_VENDORS = {
    "ntt data": "NTT Data",
    "ntt docomo": "NTT DOCOMO",
    "ntt": "NTT Data",
    "cdw": "CDW",
    "shi": "SHI",
    "pc connection": "PC Connection",
    "microsoft": "Microsoft",
    "msft": "Microsoft",
    "trendmicro": "TrendMicro",
    "trend micro": "TrendMicro",
    "knowbe4": "KnowBe4",
    "equinix": "Equinix",
    "servicenow": "ServiceNow",
    "service-now": "ServiceNow",
    "quest": "Quest",
    "proquire": "Proquire LLC",
    "honeywell": "Honeywell",
    "thrive": "Thrive",
    "copeland": "Copeland LP",
    "cisco": "Cisco",
    "palo alto": "Palo Alto",
    "zscaler": "Zscaler",
    "cyberark": "CyberArk",
    "forescout": "Forescout",
    "vmware": "VMware",
    "oracle": "Oracle",
    "netapp": "NetApp",
    "ibm": "IBM",
    "pure storage": "Pure Storage",
}

SERVICE_KEYWORDS = {
    # Cybersecurity
    "trend vision one": "Trend Vision One Endpoint Security",
    "apex one": "Apex One SaaS",
    "trend micro email": "Trend Micro Email Security Advanced",
    "cloud app security": "Cloud App Security XDR",
    "knowbe4 phisher": "KnowBe4 PhishER Subscription",
    "security awareness": "Security Awareness Training",
    "zscaler zia": "Zscaler ZIA Transformation Edition",
    "zero trust network": "Zero Trust Network Access",
    "zscaler internet": "Zscaler Internet Access",
    "cyberark secure": "CyberArk Secure IT Ops Standard",
    "privileged access": "Privileged Access Management",
    "cyberark endpoint": "CyberArk Endpoint Privilege Manager",
    "forescout": "Forescout Network Access Control",
    "endpoint visibility": "Endpoint Visibility",
    # Network & Telecom
    "catalyst c8300": "Cisco Catalyst C8300",
    "c8300": "Cisco Catalyst C8300",
    "catalyst 9800": "Cisco Catalyst 9800-L",
    "9800-l": "Cisco Catalyst 9800-L",
    "catalyst 9200": "Cisco Catalyst 9200-L",
    "catalyst 9400": "Cisco Catalyst 9400",
    "catalyst 8500": "Cisco Catalyst 8500",
    "wireless 9176": "Cisco Wireless 9176I",
    "9176i": "Cisco Wireless 9176I",
    "nexus 9200": "Cisco Nexus 9200L",
    "nexus 9300": "Cisco Nexus 9300",
    "nexus 9000": "Cisco Nexus 9000",
    "meraki mr46": "Cisco Meraki MR46E",
    "smartnet": "Cisco SMARTnet",
    "firepower 1150": "Cisco Firepower 1150 NGFW",
    "pa 445": "Palo Alto PA 445",
    "pa-445": "Palo Alto PA 445",
    "ngfw firewall": "Palo Alto NGFW Firewall",
    "equinix": "Equinix Network Interconnect Lines",
    "global wan": "Global WAN Connectivity",
    "cloud connectivity": "Cloud Connectivity",
    # Hosting
    "vmware cloud foundation": "VMware Cloud Foundation",
    "vmware vsphere": "VMware vSphere Enterprise",
    "mds9148": "Cisco MDS9148T",
    "netapp aff": "NetApp AFF A30 HA System",
    "oracle database appliance": "Oracle Database Appliance X11-L",
    "oracle database enterprise": "Oracle Database Enterprise Edition",
    "colocation power": "Colocation Power and Space",
    "internet access": "Internet Access",
    "windows server": "Microsoft Windows Server Datacenter",
    "sql server": "Microsoft SQL Server Standard Core",
    "ibm power9": "IBM Power9 Server",
    "pure storage": "Pure Storage FlashArray",
    "data center build": "Data Center Build Services",
    # M365
    "m365 e5": "M365 E5 License",
    "m365 e3": "M365 E3 License",
    "m365 f3": "M365 F3 License",
    "o365 e1": "O365 E1 License",
    "visio p1": "Visio P1",
    "visio p2": "Visio P2",
    "windows 365": "Windows 365",
    "microsoft defender": "Microsoft Defender",
    "power platform": "Power Platform",
    "power bi premium": "Power BI Premium",
    "power bi pro": "Power BI Pro",
    "m365 copilot": "M365 Copilot",
    "teams essentials": "Teams Essentials",
    # Service Management
    "servicenow itsm": "ServiceNow IT Service Management Professional",
    "servicenow it service management": "ServiceNow IT Service Management Professional",
    "servicenow app engine": "ServiceNow App Engine Enterprise",
    "servicenow itom": "ServiceNow IT Operations Management",
    # IdAM
    "on-demand migration suite t5": "Quest On-Demand Migration Suite T5",
    "odm t5": "Quest On-Demand Migration Suite T5",
    "odmt5": "Quest On-Demand Migration Suite T5",
    "active directory migration": "Active Directory Migration",
    "quest professional": "Quest Professional Services",
    "on-demand migration m365": "Quest On-Demand Migration M365",
}

CATEGORY_RULES = {
    "Cybersecurity": [
        "trend", "apex", "knowbe4", "phishing", "zscaler", "cyberark",
        "forescout", "ngfw", "firepower", "endpoint", "xdr", "edr",
        "security awareness", "privileged access", "zero trust",
    ],
    "Network & Telecom": [
        "cisco", "catalyst", "nexus", "meraki", "wireless", "smartnet",
        "palo alto", "router", "switch", "wan", "lan", "equinix",
        "interconnect", "network",
    ],
    "Hosting": [
        "vmware", "vsphere", "netapp", "oracle database", "colocation",
        "datacenter", "data center", "windows server", "sql server",
        "ibm power", "pure storage", "storage array", "hosting",
    ],
    "M365 & Power Platform": [
        "m365", "o365", "office 365", "microsoft 365", "visio",
        "windows 365", "defender", "power platform", "power bi",
        "copilot", "teams essentials",
    ],
    "Service Management (SNow)": [
        "servicenow", "service-now", "itsm", "itom", "app engine",
    ],
    "IdAM": [
        "active directory", "ad migration", "odm", "on-demand migration",
        "quest", "identity", "sso", "iam",
    ],
}

COUNTRY_KEYWORDS = {
    "germany": ("Germany", "EMEA"),
    "deutschland": ("Germany", "EMEA"),
    "japan": ("Japan", "APAC"),
    "india": ("India", "APAC"),
    "singapore": ("Singapore", "APAC"),
    "malaysia": ("Malaysia", "APAC"),
    "united states": ("United States", "Americas"),
    "usa": ("United States", "Americas"),
    "u.s.": ("United States", "Americas"),
    "us ": ("United States", "Americas"),
    "czech republic": ("Czech Republic", "EMEA"),
    "czech": ("Czech Republic", "EMEA"),
    "global": ("Multi-Region", "Global"),
    "multi-region": ("Multi-Region", "Global"),
    "worldwide": ("Multi-Region", "Global"),
}


# ============================================================================
# SKU & QUANTITY EXTRACTION HELPERS (NEW)
# ============================================================================

# Common SKU patterns for major vendors
SKU_PATTERNS = [
    # Cisco SKUs (e.g., C9300-48P-A, N9K-C9336C-FX2, FPR1150-NGFW-K9)
    r'\b([A-Z]{1,4}\d{4,5}[A-Z0-9]?-[A-Z0-9]{2,8}(?:-[A-Z0-9]{1,5})?)\b',
    # Microsoft SKUs (e.g., CFQ7TTC0LF8R, MSFT-WS-DC)
    r'\b(CFQ7[A-Z0-9]{8})\b',
    r'\b(MSFT-[A-Z]{2,5}-[A-Z]{2,5})\b',
    # Generic vendor SKU (e.g., ABC-DEF-123, TM-VO-EP-STD)
    r'\b([A-Z]{2,5}-[A-Z0-9]{2,8}(?:-[A-Z0-9]{1,8}){0,3})\b',
    # Numeric SKUs (e.g., 730-12345-AB)
    r'\b(\d{3}-\d{4,6}-[A-Z]{2,4})\b',
]

QUANTITY_KEYWORDS = ['qty', 'quantity', 'units', 'seats', 'licenses', 'count', 'amount', 'no.', 'nos.']
UNIT_PRICE_KEYWORDS = ['unit price', 'price/unit', 'rate', 'each', 'per seat', 'per unit', 'unit cost']
LINE_TOTAL_KEYWORDS = ['line total', 'extended', 'subtotal', 'amount', 'total price']


def extract_sku(text: str, vendor: str = '') -> str:
    """
    Extract SKU/Part Number from text.
    Returns the first matching SKU pattern.
    """
    if not text:
        return ''
    
    for pattern in SKU_PATTERNS:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    
    return ''


def generate_sku(vendor: str, service_name: str) -> str:
    """Generate a fallback SKU when none is found in the document."""
    if not vendor or not service_name:
        return 'UNKNOWN-SKU'
    
    v_prefix = re.sub(r'[^A-Z]', '', vendor.upper())[:3]
    s_words = service_name.upper().split()[:2]
    s_prefix = '-'.join(re.sub(r'[^A-Z0-9]', '', w)[:3] for w in s_words if w)
    
    if not v_prefix:
        v_prefix = 'GEN'
    if not s_prefix:
        s_prefix = 'SVC'
    
    return f"{v_prefix}-{s_prefix}"


def extract_quantity(text: str, default: int = 1) -> int:
    """
    Extract quantity from text. Looks for patterns like "Qty: 100", "100 units", etc.
    """
    if not text:
        return default
    
    # Pattern: "Qty 100", "Qty: 100", "Quantity: 100"
    for kw in QUANTITY_KEYWORDS:
        pattern = rf'{kw}\s*[:=]?\s*(\d+(?:,\d{{3}})*)'
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return int(match.group(1).replace(',', ''))
    
    # Pattern: "100 units", "100 seats"
    pattern = r'(\d+(?:,\d{3})*)\s*(?:units?|seats?|licenses?|users?)'
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return int(match.group(1).replace(',', ''))
    
    return default


def extract_unit_price(text: str) -> float:
    """Extract unit price from text."""
    if not text:
        return 0.0
    
    for kw in UNIT_PRICE_KEYWORDS:
        # Pattern: "Unit Price: $123.45" or "$123.45 per unit"
        pattern = rf'{kw}\s*[:=]?\s*\$?\s*([\d,]+\.?\d*)'
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                return float(match.group(1).replace(',', ''))
            except ValueError:
                continue
    
    return 0.0


def calculate_line_economics(qty: int, unit_price: float, line_total: float) -> dict:
    """
    Auto-calculate missing values from the available ones.
    If you have any 2 of (qty, unit_price, line_total), derive the third.
    """
    result = {'qty': qty, 'unitPrice': unit_price, 'lineTotal': line_total}
    
    if qty and unit_price and not line_total:
        result['lineTotal'] = round(qty * unit_price, 2)
    elif line_total and qty and not unit_price:
        result['unitPrice'] = round(line_total / qty, 2) if qty > 0 else 0
    elif line_total and unit_price and not qty:
        result['qty'] = round(line_total / unit_price) if unit_price > 0 else 1
    
    # Sanity defaults
    if not result['qty'] or result['qty'] <= 0:
        result['qty'] = 1
    if not result['unitPrice'] or result['unitPrice'] <= 0:
        if result['lineTotal'] and result['qty']:
            result['unitPrice'] = round(result['lineTotal'] / result['qty'], 2)
        else:
            result['unitPrice'] = 0
    if not result['lineTotal']:
        result['lineTotal'] = round(result['qty'] * result['unitPrice'], 2)
    
    return result


def build_service_object(name: str, vendor: str = '', text_context: str = '', 
                         qty: int = None, unit_price: float = None, 
                         line_total: float = None) -> dict:
    """
    Build a complete service object with SKU, qty, unitPrice, lineTotal.
    All fields are auto-derived where possible.
    """
    # Try to find SKU in surrounding context
    sku = extract_sku(text_context, vendor)
    if not sku:
        sku = generate_sku(vendor, name)
    
    # Use provided values or extract from context
    final_qty = qty if qty is not None else extract_quantity(text_context, 1)
    final_unit = unit_price if unit_price is not None else extract_unit_price(text_context)
    final_line = line_total if line_total is not None else 0.0
    
    economics = calculate_line_economics(final_qty, final_unit, final_line)
    
    return {
        'name': name,
        'sku': sku,
        'qty': economics['qty'],
        'unitPrice': economics['unitPrice'],
        'lineTotal': economics['lineTotal']
    }

# ============================================================================
# VENDOR EXTRACTION
# ============================================================================

def extract_vendor(text: str, filename: str) -> str:
    """Identify vendor from text content and filename."""
    haystack = (filename + " " + text[:5000]).lower()
    
    # Score each vendor by occurrence count in first 5000 chars
    scores = Counter()
    for keyword, canonical in KNOWN_VENDORS.items():
        count = haystack.count(keyword)
        if count > 0:
            scores[canonical] += count
    
    if scores:
        return scores.most_common(1)[0][0]
    return "Unknown"


# ============================================================================
# SERVICE EXTRACTION
# ============================================================================

def extract_services(text: str) -> List[str]:
    """Extract service names mentioned in the document."""
    text_lower = text.lower()
    found = set()
    for keyword, canonical in SERVICE_KEYWORDS.items():
        if keyword in text_lower:
            found.add(canonical)
    return sorted(found)


# ============================================================================
# PRICE EXTRACTION
# ============================================================================

PRICE_PATTERNS = [
    r"(?:total|grand\s*total|sum|amount|price)[\s:]*\$?\s*([\d,]+(?:\.\d{2})?)",
    r"\$\s*([\d,]+(?:\.\d{2})?)",
    r"(?:USD|EUR|GBP)\s*([\d,]+(?:\.\d{2})?)",
]

def extract_price(text: str) -> int:
    """Extract the largest dollar amount as the total quote price."""
    candidates = []
    for pat in PRICE_PATTERNS:
        for m in re.finditer(pat, text, re.IGNORECASE):
            try:
                val = float(m.group(1).replace(",", ""))
                if 100 <= val <= 50_000_000:  # sanity bounds
                    candidates.append(val)
            except (ValueError, IndexError):
                continue
    if not candidates:
        return 0
    # Use the largest value as it's likely the total
    return int(max(candidates))


# ============================================================================
# LINE ITEM EXTRACTION (SKU + Qty + Unit Price + Line Total)
# ============================================================================

def _is_valid_sku(candidate: str) -> bool:
    """Reject obviously non-SKU strings."""
    if not candidate or len(candidate) < 4:
        return False
    if candidate.upper() in SKU_BLACKLIST:
        return False
    # Reject pure years (2024, 2025, etc.)
    if re.match(r"^(19|20)\d{2}$", candidate):
        return False
    # Must contain at least one digit and one letter (or dash/underscore)
    has_digit = any(c.isdigit() for c in candidate)
    has_alpha = any(c.isalpha() for c in candidate)
    return has_digit and has_alpha


def _find_sku_in_line(line: str) -> str:
    """Find the first valid SKU pattern in a line."""
    for pat in SKU_PATTERNS:
        m = re.search(pat, line)
        if m:
            sku = m.group(1)
            if _is_valid_sku(sku):
                return sku
    return ""


def _find_service_in_line(line: str, services_found: List[str]) -> str:
    """Match a known service against this line."""
    line_low = line.lower()
    # Try exact service match first
    for svc in services_found:
        if svc.lower() in line_low:
            return svc
    # Try keyword match (the original keywords from SERVICE_KEYWORDS)
    for keyword, canonical in SERVICE_KEYWORDS.items():
        if keyword in line_low and canonical in services_found:
            return canonical
    return ""


def _extract_numbers(line: str) -> List[float]:
    """Extract all numeric values from a line."""
    nums = []
    for n in re.findall(r"[\d,]+(?:\.\d{1,4})?", line):
        try:
            v = float(n.replace(",", ""))
            if v > 0:
                nums.append(v)
        except ValueError:
            continue
    return nums


def _find_qty_unit_total(numbers: List[float]) -> Tuple[int, float, int]:
    """
    Given a list of numbers from a line, try to identify which is qty,
    which is unit_price, and which is line_total such that qty × unit ≈ total.
    
    Returns (qty, unit_price, line_total) or (None, None, None) if no match.
    """
    if len(numbers) < 2:
        return None, None, None
    
    sorted_nums = sorted(numbers, reverse=True)
    
    # Try all triplet combinations to find qty × unit = total (within 2% tolerance)
    if len(sorted_nums) >= 3:
        for i, a in enumerate(sorted_nums):
            if a < 100:
                continue  # total should be reasonably large
            for j, b in enumerate(sorted_nums):
                if i == j:
                    continue
                for k, c in enumerate(sorted_nums):
                    if k == i or k == j:
                        continue
                    product = b * c
                    if product > 0 and abs(product - a) / a < 0.02:
                        # b and c are qty/unit; a is total
                        # qty is usually integer-like
                        if b == int(b) and 1 <= b <= 1_000_000:
                            return int(b), c, int(a)
                        elif c == int(c) and 1 <= c <= 1_000_000:
                            return int(c), b, int(a)
                        else:
                            # Pick smaller as qty
                            return int(min(b, c)), max(b, c), int(a)
    
    # Fallback: 2 numbers — one is qty (integer), other is line_total or unit_price
    if len(sorted_nums) == 2:
        a, b = sorted_nums[0], sorted_nums[1]
        if b == int(b) and 1 <= b <= 1_000_000 and a > b:
            qty = int(b)
            unit = a / qty if qty > 0 else 0
            return qty, round(unit, 2), int(a)
    
    return None, None, None


def extract_line_items(text: str, services_found: List[str]) -> List[Dict]:
    """
    Walk through every line of the document and try to extract:
      service · sku · qty · unit_price · line_total · unit_type
    
    Returns deduplicated list of line items (max 50).
    """
    lines_out = []
    
    for raw_line in text.split("\n"):
        line = raw_line.strip()
        if len(line) < 10:
            continue
        
        sku = _find_sku_in_line(line)
        service = _find_service_in_line(line, services_found)
        
        # If neither SKU nor service is found, skip this line
        if not sku and not service:
            continue
        
        numbers = _extract_numbers(line)
        if len(numbers) < 2:
            continue
        
        qty, unit_price, line_total = _find_qty_unit_total(numbers)
        
        # Sanity filter
        if qty is not None and (qty < 1 or qty > 1_000_000):
            qty = None
        if unit_price is not None and (unit_price < 0.01 or unit_price > 10_000_000):
            unit_price = None
        if line_total is not None and line_total < 1:
            line_total = None
        
        # Need at least qty + unit_price OR line_total to be useful
        if not (qty and unit_price) and not line_total:
            continue
        
        # Detect unit type heuristically
        line_low = line.lower()
        unit_type = "per unit"
        if "per user" in line_low or "/user" in line_low:
            unit_type = "per user/year" if "year" in line_low or "annual" in line_low else "per user/month"
        elif "per device" in line_low or "/device" in line_low:
            unit_type = "per device"
        elif "per seat" in line_low or "/seat" in line_low:
            unit_type = "per seat"
        elif "monthly" in line_low or "/month" in line_low:
            unit_type = "per month"
        elif "annual" in line_low or "/year" in line_low or "yearly" in line_low:
            unit_type = "per year"
        
        lines_out.append({
            "service": service or "Unknown Service",
            "sku": sku or "",
            "description": line[:140],
            "qty": qty or 0,
            "unit_price": round(unit_price, 2) if unit_price else 0,
            "line_total": int(line_total) if line_total else 0,
            "unit_type": unit_type,
        })
    
    # Deduplicate by (sku, service, qty, unit_price)
    seen = set()
    deduped = []
    for ln in lines_out:
        key = (ln["sku"], ln["service"], ln["qty"], ln["unit_price"])
        if key not in seen:
            seen.add(key)
            deduped.append(ln)
    
    return deduped[:50]


# ============================================================================
# COUNTRY / REGION
# ============================================================================

def extract_country_region(text: str, filename: str) -> Tuple[str, str]:
    """Identify primary country and region."""
    haystack = (filename + " " + text[:3000]).lower()
    for keyword, (country, region) in COUNTRY_KEYWORDS.items():
        if keyword in haystack:
            return country, region
    return "Unknown", "Global"


# ============================================================================
# YEAR / QUARTER
# ============================================================================

def extract_year(text: str, filename: str) -> int:
    """Extract the most likely year (YYYY between 2020-2030)."""
    haystack = filename + " " + text[:3000]
    years = []
    for m in re.finditer(r"\b(20[2-3]\d)\b", haystack):
        years.append(int(m.group(1)))
    if years:
        # Most recent year mentioned wins
        return max(set(years), key=years.count)
    return 2025  # default


def extract_quarter(text: str, filename: str) -> str:
    """Extract Q1/Q2/Q3/Q4."""
    haystack = (filename + " " + text[:2000]).lower()
    m = re.search(r"\bq([1-4])\b", haystack)
    if m:
        return f"Q{m.group(1)}"
    # Try by month name
    months_q = {
        "january": "Q1", "february": "Q1", "march": "Q1",
        "april": "Q2", "may": "Q2", "june": "Q2",
        "july": "Q3", "august": "Q3", "september": "Q3",
        "october": "Q4", "november": "Q4", "december": "Q4",
    }
    for month, q in months_q.items():
        if month in haystack:
            return q
    # Try MM-DD pattern
    m = re.search(r"(\d{1,2})[-/](\d{1,2})", haystack)
    if m:
        try:
            month = int(m.group(1))
            if 1 <= month <= 3:
                return "Q1"
            elif 4 <= month <= 6:
                return "Q2"
            elif 7 <= month <= 9:
                return "Q3"
            elif 10 <= month <= 12:
                return "Q4"
        except ValueError:
            pass
    return "Q1"


# ============================================================================
# CATEGORY CLASSIFICATION
# ============================================================================

def categorise(text: str, filename: str, services: List[str], vendor: str) -> str:
    """Score each category by keyword frequency and pick the highest."""
    haystack = (filename + " " + " ".join(services) + " " + text[:5000]).lower()
    
    scores = Counter()
    for category, keywords in CATEGORY_RULES.items():
        for kw in keywords:
            scores[category] += haystack.count(kw)
    
    if scores and scores.most_common(1)[0][1] > 0:
        return scores.most_common(1)[0][0]
    
    return "Other"


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def heuristic_extract(text: str, filename: str) -> Dict:
    """
    Run all heuristic extractors and return a unified record.
    """
    vendor = extract_vendor(text, filename)
    services = extract_services(text)
    country, region = extract_country_region(text, filename)
    lines = extract_line_items(text, services)
    
    # Compute total from line items if available, else use text-extracted total
    line_sum = sum(ln.get("line_total", 0) for ln in lines)
    text_total = extract_price(text)
    
    # Prefer line-item total if it matches text total within 25% (sanity check)
    if line_sum > 0 and text_total > 0:
        if abs(line_sum - text_total) / max(text_total, 1) < 0.25:
            final_price = line_sum
        else:
            final_price = text_total  # discrepancy → trust the explicit total
    elif line_sum > 0:
        final_price = line_sum
    else:
        final_price = text_total
    
    return {
        "vendor": vendor,
        "price": final_price,
        "services": services,
        "lines": lines,
        "country": country,
        "region": region,
        "year": extract_year(text, filename),
        "quarter": extract_quarter(text, filename),
        "category": categorise(text, filename, services, vendor),
    }
