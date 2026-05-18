"""
extractor/heuristic_extractor.py

Heuristic extractor for IT contracting quotes.

Extracts:
  • vendor
  • category
  • country / region
  • year / quarter
  • total price
  • services (list of canonical service names)
  • lines (list of dicts with sku, service, qty, unit_price, line_total, unit_type)
  • confidence (0-100 quality score)

Designed to work on markdown text from LlamaParse, including:
  • Plain text quotes
  • Markdown tables (| col | col |)
  • Indented or tab-separated columns
"""

import re
import logging
from typing import Dict, List, Tuple, Optional
from collections import Counter

logger = logging.getLogger(__name__)


# ============================================================================
# KNOWN ENTITIES — extend these lists as you onboard more vendors/services
# ============================================================================

KNOWN_VENDORS = {
    "ntt data":        "NTT Data",
    "ntt docomo":      "NTT DOCOMO",
    "ntt":             "NTT Data",
    "cdw":             "CDW",
    "shi":             "SHI",
    "pc connection":   "PC Connection",
    "microsoft":       "Microsoft",
    "msft":            "Microsoft",
    "trendmicro":      "TrendMicro",
    "trend micro":     "TrendMicro",
    "knowbe4":         "KnowBe4",
    "equinix":         "Equinix",
    "servicenow":      "ServiceNow",
    "service-now":     "ServiceNow",
    "quest":           "Quest",
    "proquire":        "Proquire LLC",
    "honeywell":       "Honeywell",
    "thrive":          "Thrive",
    "copeland":        "Copeland LP",
    "cisco":           "Cisco",
    "palo alto":       "Palo Alto Networks",
    "paloalto":        "Palo Alto Networks",
    "zscaler":         "Zscaler",
    "cyberark":        "CyberArk",
    "forescout":       "Forescout",
    "vmware":          "VMware",
    "oracle":          "Oracle",
    "netapp":          "NetApp",
    "ibm":             "IBM",
    "pure storage":    "Pure Storage",
    "ricoh":           "Ricoh",
}

SERVICE_KEYWORDS = {
    # Cybersecurity
    "trend vision one":          "Trend Vision One Endpoint Security",
    "apex one":                  "Apex One SaaS",
    "trend micro email":         "Trend Micro Email Security Advanced",
    "cloud app security":        "Cloud App Security XDR",
    "knowbe4 phisher":           "KnowBe4 PhishER Subscription",
    "phisher":                   "KnowBe4 PhishER Subscription",
    "security awareness":        "Security Awareness Training",
    "zscaler zia":               "Zscaler ZIA Transformation Edition",
    "zero trust network":        "Zero Trust Network Access",
    "zscaler internet":          "Zscaler Internet Access",
    "cyberark secure":           "CyberArk Secure IT Ops Standard",
    "privileged access":         "Privileged Access Management",
    "cyberark endpoint":         "CyberArk Endpoint Privilege Manager",
    "forescout":                 "Forescout Network Access Control",
    "endpoint visibility":       "Endpoint Visibility",
    # Network & Telecom
    "catalyst c8300":            "Cisco Catalyst C8300",
    "c8300":                     "Cisco Catalyst C8300",
    "catalyst 9800":             "Cisco Catalyst 9800-L",
    "9800-l":                    "Cisco Catalyst 9800-L",
    "catalyst 9200":             "Cisco Catalyst 9200-L",
    "catalyst 9400":             "Cisco Catalyst 9400",
    "catalyst 8500":             "Cisco Catalyst 8500",
    "wireless 9176":             "Cisco Wireless 9176I",
    "9176i":                     "Cisco Wireless 9176I",
    "nexus 9200":                "Cisco Nexus 9200L",
    "nexus 9300":                "Cisco Nexus 9300",
    "nexus 9000":                "Cisco Nexus 9000",
    "meraki mr46":               "Cisco Meraki MR46E",
    "smartnet":                  "Cisco SMARTnet",
    "firepower 1150":            "Cisco Firepower 1150 NGFW",
    "pa 445":                    "Palo Alto PA 445",
    "pa-445":                    "Palo Alto PA 445",
    "ngfw firewall":             "Palo Alto NGFW Firewall",
    "interconnect":              "Equinix Network Interconnect Lines",
    "global wan":                "Global WAN Connectivity",
    "cloud connectivity":        "Cloud Connectivity",
    # Hosting
    "vmware cloud foundation":   "VMware Cloud Foundation",
    "vmware vsphere":            "VMware vSphere Enterprise",
    "mds9148":                   "Cisco MDS9148T",
    "netapp aff":                "NetApp AFF A30 HA System",
    "oracle database appliance": "Oracle Database Appliance X11-L",
    "oracle database enterprise":"Oracle Database Enterprise Edition",
    "colocation power":          "Colocation Power and Space",
    "internet access":           "Internet Access",
    "windows server":            "Microsoft Windows Server Datacenter",
    "sql server":                "Microsoft SQL Server Standard Core",
    "ibm power9":                "IBM Power9 Server",
    "flasharray":                "Pure Storage FlashArray",
    "data center build":         "Data Center Build Services",
    # M365
    "m365 e5":                   "M365 E5 License",
    "m365 e3":                   "M365 E3 License",
    "m365 f3":                   "M365 F3 License",
    "o365 e1":                   "O365 E1 License",
    "office 365 e1":             "O365 E1 License",
    "visio p1":                  "Visio P1",
    "visio p2":                  "Visio P2",
    "windows 365":               "Windows 365",
    "microsoft defender":        "Microsoft Defender",
    "power platform":            "Power Platform",
    "power bi premium":          "Power BI Premium",
    "power bi pro":              "Power BI Pro",
    "m365 copilot":              "M365 Copilot",
    "teams essentials":          "Teams Essentials",
    # Service Management
    "servicenow itsm":           "ServiceNow IT Service Management Professional",
    "servicenow it service":     "ServiceNow IT Service Management Professional",
    "app engine":                "ServiceNow App Engine Enterprise",
    "servicenow itom":           "ServiceNow IT Operations Management",
    "it operations management":  "ServiceNow IT Operations Management",
    # IdAM
    "on-demand migration suite t5": "Quest On-Demand Migration Suite T5",
    "odm t5":                    "Quest On-Demand Migration Suite T5",
    "odmt5":                     "Quest On-Demand Migration Suite T5",
    "active directory migration":"Active Directory Migration",
    "ad migration":              "Active Directory Migration",
    "quest professional":        "Quest Professional Services",
    "on-demand migration m365":  "Quest On-Demand Migration M365",
}

CATEGORY_RULES = {
    "Cybersecurity": [
        "trend", "apex", "knowbe4", "phishing", "phisher", "zscaler", "cyberark",
        "forescout", "ngfw", "firepower", "endpoint", "xdr", "edr",
        "security awareness", "privileged access", "zero trust",
    ],
    "Network & Telecom": [
        "cisco", "catalyst", "nexus", "meraki", "wireless", "smartnet",
        "palo alto", "router", "switch", "wan", "lan", "equinix",
        "interconnect", "network", "firewall",
    ],
    "Hosting": [
        "vmware", "vsphere", "netapp", "oracle database", "colocation",
        "datacenter", "data center", "windows server", "sql server",
        "ibm power", "pure storage", "storage array", "hosting", "rack",
    ],
    "M365 & Power Platform": [
        "m365", "o365", "office 365", "microsoft 365", "visio",
        "windows 365", "defender", "power platform", "power bi",
        "copilot", "teams essentials", "exchange online",
    ],
    "Service Management (SNow)": [
        "servicenow", "service-now", "itsm", "itom", "app engine",
    ],
    "IdAM": [
        "active directory", "ad migration", "odm", "on-demand migration",
        "quest", "identity", "sso", "iam", "privileged identity",
    ],
}

COUNTRY_KEYWORDS = {
    "germany":        ("Germany",        "EMEA"),
    "deutschland":    ("Germany",        "EMEA"),
    "japan":          ("Japan",          "APAC"),
    "tokyo":          ("Japan",          "APAC"),
    "india":          ("India",          "APAC"),
    "gurgaon":        ("India",          "APAC"),
    "mumbai":         ("India",          "APAC"),
    "singapore":      ("Singapore",      "APAC"),
    "malaysia":       ("Malaysia",       "APAC"),
    "united states":  ("United States",  "Americas"),
    "usa":            ("United States",  "Americas"),
    "u.s.":           ("United States",  "Americas"),
    "us-":            ("United States",  "Americas"),
    "czech republic": ("Czech Republic", "EMEA"),
    "czech":          ("Czech Republic", "EMEA"),
    "global":         ("Multi-Region",   "Global"),
    "multi-region":   ("Multi-Region",   "Global"),
    "worldwide":      ("Multi-Region",   "Global"),
}

# ============================================================================
# SKU PATTERNS & BLACKLIST
# ============================================================================

# Patterns for valid SKUs (longest/most-specific first)
SKU_PATTERNS = [
    # Microsoft commerce SKUs (CFQ7TTC0LF8R)
    r'\b(CFQ7[A-Z0-9]{8})\b',
    # Cisco-style: C9300-48P-A, FPR1150-NGFW-K9, N9K-C9336C-FX2
    r'\b([A-Z]{1,4}\d{3,5}[A-Z0-9]?-[A-Z0-9]{1,8}(?:-[A-Z0-9]{1,5}){0,2})\b',
    # MSFT-WS-DC, TM-VO-EP-STD style
    r'\b([A-Z]{2,5}-[A-Z0-9]{2,8}(?:-[A-Z0-9]{1,8}){1,4})\b',
    # 730-12345-AB style
    r'\b(\d{3}-\d{4,6}-[A-Z]{2,4})\b',
    # IBM-style power servers
    r'\b(IBM-[A-Z0-9-]{4,15})\b',
]

# Common false-positive words that match SKU patterns but aren't SKUs
SKU_BLACKLIST = {
    "USA", "EUR", "USD", "GBP", "JPY", "INR",
    "ITSM", "ITOM", "NGFW", "SaaS", "SAAS",
    "CFQ7", "PDF", "DOCX", "XLSX", "IPV4", "IPV6",
    "AWS", "GCP", "VPN", "MDR", "EDR", "XDR",
    "SOW", "PoC", "POC", "RFP", "RFQ", "EOL",
    "TBD", "N/A", "NA", "Q1", "Q2", "Q3", "Q4",
    "PAS", "PASJ", "PASCZ", "PASAP",
}


# ============================================================================
# SKU EXTRACTION HELPERS
# ============================================================================

def _is_valid_sku(candidate: str) -> bool:
    """Reject obviously non-SKU strings."""
    if not candidate or len(candidate) < 4 or len(candidate) > 40:
        return False
    if candidate.upper() in SKU_BLACKLIST:
        return False
    # Reject pure years (2024, 2025, etc.)
    if re.match(r"^(19|20)\d{2}$", candidate):
        return False
    # Must contain at least one digit AND one letter
    has_digit = any(c.isdigit() for c in candidate)
    has_alpha = any(c.isalpha() for c in candidate)
    return has_digit and has_alpha


def _find_sku_in_line(line: str) -> str:
    """Find the first valid SKU pattern in a line."""
    for pat in SKU_PATTERNS:
        for m in re.finditer(pat, line):
            sku = m.group(1)
            if _is_valid_sku(sku):
                return sku
    return ""


def generate_sku(vendor: str, service_name: str) -> str:
    """Generate a fallback SKU when none is found in the document."""
    if not vendor or not service_name:
        return "UNKNOWN-SKU"
    
    v_prefix = re.sub(r"[^A-Z]", "", vendor.upper())[:3]
    s_words  = service_name.upper().split()[:2]
    s_prefix = "-".join(
        re.sub(r"[^A-Z0-9]", "", w)[:4]
        for w in s_words if w
    )
    
    if not v_prefix:
        v_prefix = "GEN"
    if not s_prefix:
        s_prefix = "SVC"
    
    return f"{v_prefix}-{s_prefix}"


# ============================================================================
# VENDOR EXTRACTION
# ============================================================================

def extract_vendor(text: str, filename: str) -> str:
    """Identify vendor from text content and filename."""
    haystack = (filename + " " + text[:5000]).lower()
    
    # Score each vendor by occurrence count (with filename boost)
    scores = Counter()
    fname_low = filename.lower()
    
    for keyword, canonical in KNOWN_VENDORS.items():
        text_count = haystack.count(keyword)
        # Boost score 3x if vendor name appears in filename
        if keyword in fname_low:
            text_count *= 3
        if text_count > 0:
            scores[canonical] += text_count
    
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


def _find_service_in_line(line: str, services_found: List[str]) -> str:
    """Match a known service against this line."""
    line_low = line.lower()
    
    # Try exact canonical service name first
    for svc in services_found:
        if svc.lower() in line_low:
            return svc
    
    # Try keyword match (only return services already in services_found)
    for keyword, canonical in SERVICE_KEYWORDS.items():
        if keyword in line_low and canonical in services_found:
            return canonical
    
    return ""


# ============================================================================
# PRICE EXTRACTION
# ============================================================================

def extract_price(text: str) -> int:
    """
    Extract the largest dollar amount that's likely the total quote price.
    Prefers values appearing near 'total', 'grand total', 'sum'.
    """
    candidates = []
    
    # ── Pass 1: prioritise values near "total" keywords ────────
    total_pattern = re.compile(
        r"(?:grand\s*total|total\s*(?:amount|price|cost)?|sum)[\s:$]*([\d,]+(?:\.\d{2})?)",
        re.IGNORECASE,
    )
    for m in total_pattern.finditer(text):
        try:
            val = float(m.group(1).replace(",", ""))
            if 100 <= val <= 50_000_000:
                candidates.append((val, 2))  # higher weight (2x)
        except ValueError:
            continue
    
    # ── Pass 2: any dollar amount ──────────────────────────────
    dollar_pattern = re.compile(r"\$\s*([\d,]+(?:\.\d{2})?)")
    for m in dollar_pattern.finditer(text):
        try:
            val = float(m.group(1).replace(",", ""))
            if 100 <= val <= 50_000_000:
                candidates.append((val, 1))
        except ValueError:
            continue
    
    if not candidates:
        return 0
    
    # ── Prefer highest total-marked value, else max overall ────
    total_marked = [v for v, w in candidates if w == 2]
    if total_marked:
        return int(max(total_marked))
    
    return int(max(v for v, _ in candidates))


# ============================================================================
# LINE-ITEM EXTRACTION (table-aware)
# ============================================================================

def _extract_numbers(line: str) -> List[float]:
    """Extract all numeric values from a line (positive, > 0)."""
    nums = []
    # Strip $ signs first to avoid them being part of numbers
    cleaned = re.sub(r"[\$£€¥]", "", line)
    for n in re.findall(r"[\d,]+(?:\.\d{1,4})?", cleaned):
        try:
            v = float(n.replace(",", ""))
            if v > 0:
                nums.append(v)
        except ValueError:
            continue
    return nums


def _find_qty_unit_total(numbers: List[float]) -> Tuple[Optional[int], Optional[float], Optional[float]]:
    """
    Given a list of numbers from a line, identify which is qty, unit_price, and line_total
    such that qty × unit_price ≈ line_total.
    
    Returns (qty, unit_price, line_total) or (None, None, None) if no good match.
    """
    if len(numbers) < 2:
        return None, None, None
    
    # Filter to reasonable values
    nums = [n for n in numbers if 0.01 < n < 50_000_000]
    if len(nums) < 2:
        return None, None, None
    
    # ── Try to find a triplet (a, b, c) where a × b ≈ c (within 2%) ───
    best_match = None
    best_score = float("inf")
    
    n_count = len(nums)
    for i in range(n_count):
        for j in range(n_count):
            if i == j:
                continue
            a, b = nums[i], nums[j]
            product = a * b
            
            # Look for c that matches the product
            for k in range(n_count):
                if k == i or k == j:
                    continue
                c = nums[k]
                if c < product * 0.98 or c > product * 1.02:
                    continue
                
                # Score this match: prefer integer quantities, penalize tiny qtys
                # (which are usually ratios or percentages)
                if a == int(a) and 1 <= a <= 1_000_000:
                    qty, unit, total = int(a), b, c
                elif b == int(b) and 1 <= b <= 1_000_000:
                    qty, unit, total = int(b), a, c
                else:
                    continue  # neither operand looks like a quantity
                
                # Sanity bounds
                if qty < 1 or qty > 1_000_000:
                    continue
                if unit < 0.01 or unit > 10_000_000:
                    continue
                if total < 1:
                    continue
                
                # Lower score = better match
                deviation = abs(product - c) / c
                score = deviation
                if score < best_score:
                    best_score = score
                    best_match = (qty, round(unit, 2), round(total, 2))
    
    if best_match:
        return best_match
    
    # ── Fallback: 2 numbers — assume larger is total, smaller is qty ──
    if len(nums) == 2:
        sorted_nums = sorted(nums, reverse=True)
        a, b = sorted_nums[0], sorted_nums[1]
        if b == int(b) and 1 <= b <= 1_000_000 and a > b:
            qty = int(b)
            unit = a / qty if qty > 0 else 0
            return qty, round(unit, 2), round(a, 2)
    
    return None, None, None


def _detect_unit_type(line_low: str) -> str:
    """Heuristically detect billing unit (per user, per device, etc.)."""
    if "per user" in line_low or "/user" in line_low:
        return "per user/year" if ("year" in line_low or "annual" in line_low) else "per user/month"
    if "per device" in line_low or "/device" in line_low:
        return "per device"
    if "per seat" in line_low or "/seat" in line_low:
        return "per seat"
    if "monthly" in line_low or "/month" in line_low or "per month" in line_low:
        return "per month"
    if "annual" in line_low or "/year" in line_low or "yearly" in line_low or "per year" in line_low:
        return "per year"
    return "per unit"


def _parse_markdown_table(text: str) -> List[List[str]]:
    """
    Parse markdown-style tables into list of cell-lists.
    LlamaParse outputs tables like:
        | Col A | Col B | Col C |
        |-------|-------|-------|
        | x     | y     | z     |
    """
    rows = []
    for raw in text.split("\n"):
        line = raw.strip()
        if not line.startswith("|") or not line.endswith("|"):
            continue
        # Skip separator rows like |---|---|
        if re.match(r"^\|[\s\-:|]+\|$", line):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if cells:
            rows.append(cells)
    return rows


def _identify_table_columns(headers: List[str]) -> Dict[str, int]:
    """Identify which column index holds SKU, qty, unit_price, line_total based on headers."""
    col_map = {}
    
    for idx, header in enumerate(headers):
        h_low = header.lower().strip()
        if not h_low:
            continue
        
        # SKU column
        if any(kw in h_low for kw in ("sku", "part number", "part #", "item code", "product code", "material", "mpn", "model", "catalog")):
            col_map.setdefault("sku", idx)
        # Quantity column
        elif any(kw in h_low for kw in ("qty", "quantity", "units", "seats", "licenses", "count", "no.", "nos.")):
            col_map.setdefault("qty", idx)
        # Unit price column
        elif any(kw in h_low for kw in ("unit price", "price/unit", "rate", "each", "per seat", "per unit", "list price", "unit cost")):
            col_map.setdefault("unit_price", idx)
        # Line total column
        elif any(kw in h_low for kw in ("line total", "extended", "subtotal", "amount", "total price", "ext price", "ext. price")):
            col_map.setdefault("line_total", idx)
        # Description / service name
        elif any(kw in h_low for kw in ("description", "product", "service", "item", "name")):
            col_map.setdefault("description", idx)
    
    return col_map


def _extract_lines_from_tables(text: str, services_found: List[str], vendor: str) -> List[Dict]:
    """Extract line items from markdown tables."""
    rows = _parse_markdown_table(text)
    if len(rows) < 2:
        return []
    
    # Find a row that looks like a header
    lines_out = []
    current_col_map: Dict[str, int] = {}
    
    for row in rows:
        # Detect header row
        col_map = _identify_table_columns(row)
        if "qty" in col_map or "unit_price" in col_map or "line_total" in col_map:
            current_col_map = col_map
            continue
        
        if not current_col_map:
            continue
        
        # Process data row
        try:
            sku  = row[current_col_map["sku"]]  if "sku"  in current_col_map and current_col_map["sku"]  < len(row) else ""
            desc = row[current_col_map["description"]] if "description" in current_col_map and current_col_map["description"] < len(row) else ""
            qty_str  = row[current_col_map["qty"]]        if "qty"        in current_col_map and current_col_map["qty"]        < len(row) else "1"
            up_str   = row[current_col_map["unit_price"]] if "unit_price" in current_col_map and current_col_map["unit_price"] < len(row) else "0"
            lt_str   = row[current_col_map["line_total"]] if "line_total" in current_col_map and current_col_map["line_total"] < len(row) else "0"
        except (IndexError, KeyError):
            continue
        
        # Validate SKU
        sku = sku.strip()
        if sku and not _is_valid_sku(sku):
            # Try to find a real SKU embedded in the cell
            embedded_sku = _find_sku_in_line(sku)
            sku = embedded_sku if embedded_sku else ""
        
        # Parse numerics
        try:
            qty = int(re.sub(r"[^\d]", "", qty_str) or "0")
        except ValueError:
            qty = 0
        
        try:
            up = float(re.sub(r"[^\d.]", "", up_str) or "0")
        except ValueError:
            up = 0.0
        
        try:
            lt = float(re.sub(r"[^\d.]", "", lt_str) or "0")
        except ValueError:
            lt = 0.0
        
        # Skip rows with no useful data
        if qty <= 0 and up <= 0 and lt <= 0:
            continue
        
        # Auto-derive missing math
        if qty > 0 and up > 0 and lt <= 0:
            lt = round(qty * up, 2)
        elif qty > 0 and lt > 0 and up <= 0:
            up = round(lt / qty, 2)
        elif up > 0 and lt > 0 and qty <= 0:
            qty = max(1, round(lt / up))
        
        # Skip if we still can't derive enough
        if qty <= 0 or up <= 0:
            continue
        
        # Identify service
        full_text = f"{desc} {sku}".strip()
        service = _find_service_in_line(full_text, services_found)
        if not service:
            # Try to use the description as the service name if it's reasonable
            cleaned_desc = desc.strip()[:80]
            if cleaned_desc and len(cleaned_desc) >= 5:
                service = cleaned_desc
            else:
                service = "Unknown Service"
        
        # Generate SKU if we didn't find one
        if not sku:
            sku = generate_sku(vendor, service)
        
        unit_type = _detect_unit_type(full_text.lower())
        
        lines_out.append({
            "service":    service,
            "sku":        sku,
            "description": full_text[:140],
            "qty":        qty,
            "unit_price": round(up, 2),
            "line_total": round(lt, 2),
            "unit_type":  unit_type,
        })
    
    return lines_out


def _extract_lines_from_text(text: str, services_found: List[str], vendor: str) -> List[Dict]:
    """Fallback: extract line items by scanning each text line."""
    lines_out = []
    
    for raw_line in text.split("\n"):
        line = raw_line.strip()
        if len(line) < 10 or len(line) > 500:
            continue
        
        sku = _find_sku_in_line(line)
        service = _find_service_in_line(line, services_found)
        
        # Need at least one anchor (SKU or known service)
        if not sku and not service:
            continue
        
        numbers = _extract_numbers(line)
        if len(numbers) < 2:
            continue
        
        qty, unit_price, line_total = _find_qty_unit_total(numbers)
        
        # Sanity filter
        if qty is None and line_total is None:
            continue
        if qty is not None and (qty < 1 or qty > 1_000_000):
            qty = None
        if unit_price is not None and (unit_price < 0.01 or unit_price > 10_000_000):
            unit_price = None
        if line_total is not None and line_total < 1:
            line_total = None
        
        # Need at least qty + unit_price OR line_total
        if not (qty and unit_price) and not line_total:
            continue
        
        # Auto-derive missing
        if qty and unit_price and not line_total:
            line_total = round(qty * unit_price, 2)
        elif qty and line_total and not unit_price:
            unit_price = round(line_total / qty, 2)
        elif unit_price and line_total and not qty:
            qty = max(1, round(line_total / unit_price))
        
        if not qty or not unit_price:
            continue
        
        unit_type = _detect_unit_type(line.lower())
        
        # Generate SKU if missing
        final_sku = sku or generate_sku(vendor, service or "")
        
        lines_out.append({
            "service":    service or "Unknown Service",
            "sku":        final_sku,
            "description": line[:140],
            "qty":        qty,
            "unit_price": round(unit_price, 2),
            "line_total": round(line_total, 2),
            "unit_type":  unit_type,
        })
    
    return lines_out


def extract_line_items(text: str, services_found: List[str], vendor: str = "") -> List[Dict]:
    """
    Walk through the document and extract line items.
    Tries markdown table parsing first (LlamaParse-friendly), then falls back to line-by-line scanning.
    
    Returns deduplicated list of line items (max 60).
    """
    lines_out = []
    
    # ── Try table extraction first ─────────────────────────────
    table_lines = _extract_lines_from_tables(text, services_found, vendor)
    lines_out.extend(table_lines)
    
    # ── Fall back to line-by-line if tables yielded nothing ────
    if not lines_out:
        text_lines = _extract_lines_from_text(text, services_found, vendor)
        lines_out.extend(text_lines)
    
    # ── Deduplicate ────────────────────────────────────────────
    seen = set()
    deduped = []
    for ln in lines_out:
        key = (ln["sku"], ln["service"], ln["qty"], ln["unit_price"])
        if key not in seen:
            seen.add(key)
            deduped.append(ln)
    
    return deduped[:60]


# ============================================================================
# COUNTRY / REGION
# ============================================================================

def extract_country_region(text: str, filename: str) -> Tuple[str, str]:
    """Identify primary country and region."""
    haystack = (filename + " " + text[:3000]).lower()
    
    scores = Counter()
    for keyword, (country, region) in COUNTRY_KEYWORDS.items():
        count = haystack.count(keyword)
        if count > 0:
            scores[(country, region)] += count
    
    if scores:
        return scores.most_common(1)[0][0]
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
        # Most-mentioned year wins
        most_common = Counter(years).most_common(1)[0][0]
        return most_common
    return 2025


def extract_quarter(text: str, filename: str) -> str:
    """Extract Q1/Q2/Q3/Q4."""
    haystack = (filename + " " + text[:2000]).lower()
    
    # Direct Q1/Q2/Q3/Q4 mention
    m = re.search(r"\bq([1-4])\b", haystack)
    if m:
        return f"Q{m.group(1)}"
    
    # Month name → quarter
    months_q = {
        "january": "Q1", "february": "Q1", "march": "Q1",
        "april":   "Q2", "may":      "Q2", "june":  "Q2",
        "july":    "Q3", "august":   "Q3", "september": "Q3",
        "october": "Q4", "november": "Q4", "december":  "Q4",
    }
    found = Counter()
    for month, q in months_q.items():
        c = haystack.count(month)
        if c > 0:
            found[q] += c
    if found:
        return found.most_common(1)[0][0]
    
    # Numeric MM/DD or MM-DD
    m = re.search(r"\b(\d{1,2})[-/](\d{1,2})\b", haystack)
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
# CONFIDENCE SCORING
# ============================================================================

def compute_confidence(record: Dict) -> int:
    """
    Compute a confidence score (0-100) based on the completeness of extraction.
    """
    score = 100
    
    # Major penalties
    if record.get("vendor") == "Unknown":
        score -= 30
    if not record.get("services"):
        score -= 20
    if not record.get("lines"):
        score -= 25
    if record.get("price", 0) <= 0:
        score -= 20
    if record.get("category") == "Other":
        score -= 15
    if record.get("country") == "Unknown":
        score -= 5
    
    # Reward line-item quality
    lines = record.get("lines", [])
    if lines:
        with_sku = sum(1 for ln in lines if ln.get("sku") and not ln["sku"].startswith("UNKNOWN") and "-" in ln["sku"])
        sku_pct = with_sku / len(lines)
        if sku_pct < 0.3:
            score -= 10
        elif sku_pct < 0.6:
            score -= 5
    
    # Math consistency check
    line_sum = sum(ln.get("line_total", 0) for ln in lines)
    total = record.get("price", 0)
    if total > 0 and line_sum > 0:
        deviation = abs(line_sum - total) / total
        if deviation > 0.20:
            score -= 10
        elif deviation > 0.10:
            score -= 5
    
    return max(0, min(100, score))


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def heuristic_extract(text: str, filename: str) -> Dict:
    """
    Run all heuristic extractors and return a unified record.
    
    Returns:
      {
        "vendor":     "NTT Data",
        "category":   "Network & Telecom",
        "country":    "Germany",
        "region":     "EMEA",
        "year":       2025,
        "quarter":    "Q2",
        "price":      62400,
        "services":   ["Cisco Catalyst C8300", ...],
        "lines": [
          {
            "service":    "Cisco Catalyst C8300",
            "sku":        "C8300-1N1S-4T2X",
            "description": "Cisco Catalyst C8300 router with 4-port SFP+",
            "qty":        8,
            "unit_price": 3200.00,
            "line_total": 25600.00,
            "unit_type":  "per unit"
          },
          ...
        ],
        "confidence": 87
      }
    """
    if not text:
        return _empty_record(filename)
    
    vendor          = extract_vendor(text, filename)
    services        = extract_services(text)
    country, region = extract_country_region(text, filename)
    lines           = extract_line_items(text, services, vendor)
    
    # ── Compute total price ────────────────────────────────────
    line_sum   = sum(ln.get("line_total", 0) for ln in lines)
    text_total = extract_price(text)
    
    # Prefer line-item total if it matches text total within 25%, else trust text total
    if line_sum > 0 and text_total > 0:
        if abs(line_sum - text_total) / max(text_total, 1) < 0.25:
            final_price = line_sum
        else:
            final_price = text_total
    elif line_sum > 0:
        final_price = line_sum
    else:
        final_price = text_total
    
    # ── Build record ───────────────────────────────────────────
    record = {
        "vendor":   vendor,
        "price":    int(round(final_price)) if final_price else 0,
        "services": services,
        "lines":    lines,
        "country":  country,
        "region":   region,
        "year":     extract_year(text, filename),
        "quarter":  extract_quarter(text, filename),
        "category": categorise(text, filename, services, vendor),
    }
    
    # Add confidence score
    record["confidence"] = compute_confidence(record)
    
    return record


def _empty_record(filename: str) -> Dict:
    """Return a minimal record when no text is available."""
    return {
        "vendor":     "Unknown",
        "price":      0,
        "services":   [],
        "lines":      [],
        "country":    "Unknown",
        "region":     "Global",
        "year":       2025,
        "quarter":    "Q1",
        "category":   "Other",
        "confidence": 0,
    }
