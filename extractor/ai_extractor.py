# extractor/ai_extractor.py
# Complete rewrite for per-license extraction

import re
import json
import asyncio
from anthropic import Anthropic
from llama_cloud import AsyncLlamaCloud
from config import (
    ANTHROPIC_API_KEY,
    LLAMA_API_KEY,
    CLAUDE_MODEL,
    CLAUDE_MAX_TOKENS,
    LLAMA_TIER,
    LLAMA_EXPAND,
    KNOWN_VENDORS,
    KNOWN_SERVICES,
    MIN_VALID_PRICE,
    MAX_VALID_PRICE,
)


class AIExtractor:

    def __init__(self):
        self.claude = Anthropic(api_key=ANTHROPIC_API_KEY)
        self.llama  = AsyncLlamaCloud(api_key=LLAMA_API_KEY)
        self.stats  = {
            "llama_success":  0,
            "llama_failed":   0,
            "claude_success": 0,
            "claude_failed":  0,
            "regex_fallback": 0,
        }

    # ══════════════════════════════════════
    #  LLAMACLOUD
    # ══════════════════════════════════════
    async def parse_pdf_with_llama(self, file_bytes, filename):
        try:
            print(f"    📄 LlamaCloud: {filename[:40]}...")
            file_obj = await self.llama.files.create(
                file=(filename, file_bytes, "application/pdf"),
                purpose="parse"
            )
            result = await self.llama.parsing.parse(
                file_id=file_obj.id,
                tier=LLAMA_TIER,
                expand=LLAMA_EXPAND,
            )
            markdown = result.markdown_full or ""
            if markdown and len(markdown) > 100:
                self.stats["llama_success"] += 1
                print(f"    ✅ LlamaCloud: {len(markdown):,} chars")
                return markdown
            self.stats["llama_failed"] += 1
            return ""
        except Exception as e:
            self.stats["llama_failed"] += 1
            print(f"    ⚠️  LlamaCloud failed: {e}")
            return ""

    # ══════════════════════════════════════
    #  CLAUDE — PER LICENSE EXTRACTION
    # ══════════════════════════════════════
    def extract_with_claude(self, text, filename, category):
        """
        Extract per-license/per-unit pricing.
        Each service gets its own unit price.
        """
        text_truncated = text[:8000]
        vendor_hints   = ", ".join(KNOWN_VENDORS[:15])
        service_hints  = ", ".join(KNOWN_SERVICES[:20])

        prompt = f"""You are a specialist IT contract pricing analyst at PwC.
Extract per-unit/per-license pricing from this vendor quote.

DOCUMENT INFO:
Filename: {filename}
Category: {category}

KNOWN VENDORS: {vendor_hints}
KNOWN SERVICES: {service_hints}

DOCUMENT:
{text_truncated}

CRITICAL INSTRUCTIONS:
1. Extract EACH service/product as a SEPARATE item
2. For each item get the UNIT PRICE (price per license/seat/unit)
   NOT the total line price
3. If only total price exists divide by quantity to get unit price
4. Identify vendor, category, year, quarter

Return ONLY valid JSON in this exact format:
{{
  "vendor": "Exact Vendor Name",
  "category": "one of: Cybersecurity / Network & Telecom / Hosting / M365 & Power Platform / IdAM / Service Management (SNow)",
  "year": 2025,
  "quarter": "Q2",
  "currency": "USD",
  "confidence": "high",
  "line_items": [
    {{
      "service": "Exact Service/Product Name",
      "unit_price": 57.00,
      "quantity": 1000,
      "unit": "per license/per seat/per device/per month/per year",
      "total_line_price": 57000.00,
      "description": "brief description"
    }}
  ]
}}

RULES:
- unit_price: price per ONE license/seat/device
- quantity: number of licenses/seats/devices
- unit: what the unit_price covers
- If annual price divide by 12 for monthly
- If cannot determine unit price use total and note unit as "per quote"
- Return ONLY the JSON nothing else"""

        try:
            response = self.claude.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=CLAUDE_MAX_TOKENS,
                messages=[{"role": "user", "content": prompt}]
            )
            raw    = response.content[0].text.strip()
            result = self._parse_json_response(raw)
            if result and result.get("line_items"):
                self.stats["claude_success"] += 1
                return result
            self.stats["claude_failed"] += 1
            return self._empty_result()
        except Exception as e:
            self.stats["claude_failed"] += 1
            print(f"    ⚠️  Claude failed: {e}")
            return self._empty_result()

    def _parse_json_response(self, raw_text):
        try:
            return json.loads(raw_text)
        except json.JSONDecodeError:
            pass
        json_match = re.search(r'\{.*\}', raw_text, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group())
            except json.JSONDecodeError:
                pass
        try:
            fixed = re.sub(r',\s*}', '}', raw_text)
            fixed = re.sub(r',\s*]', ']', fixed)
            return json.loads(fixed)
        except json.JSONDecodeError:
            pass
        return None

    def _empty_result(self):
        return {
            "vendor":     "Unknown",
            "category":   "",
            "year":       2025,
            "quarter":    "Q1",
            "currency":   "USD",
            "confidence": "low",
            "line_items": [],
        }

    # ══════════════════════════════════════
    #  REGEX FALLBACKS
    # ══════════════════════════════════════
    def extract_price_regex(self, text):
        prices = []
        patterns = [
            r'(?:total|amount|price|quote|cost|value)'
            r'[:\s$]*(?:USD\s*)?([\d,]+(?:\.\d{1,2})?)',
            r'USD\s*([\d,]+(?:\.\d{1,2})?)',
            r'\$\s*([\d,]{4,}(?:\.\d{1,2})?)',
        ]
        for pattern in patterns:
            for m in re.finditer(pattern, text, re.IGNORECASE):
                try:
                    n = float(m.group(1).replace(',', ''))
                    if MIN_VALID_PRICE <= n <= MAX_VALID_PRICE:
                        prices.append(n)
                except (ValueError, IndexError):
                    continue
        if prices:
            self.stats["regex_fallback"] += 1
            prices.sort(reverse=True)
            return prices[0]
        return 0

    def extract_vendor_regex(self, text, filename):
        text_lower  = text.lower()
        fname_lower = filename.lower()
        for vendor in KNOWN_VENDORS:
            if (vendor.lower() in text_lower
                    or vendor.lower() in fname_lower):
                return vendor
        return "Unknown"

    def extract_services_regex(self, text):
        found   = []
        t_lower = text.lower()
        for service in KNOWN_SERVICES:
            if service.lower() in t_lower:
                found.append(service)
        return list(set(found))

    def extract_year_regex(self, text, filename):
        years = re.findall(r'\b(202[0-9])\b', text + " " + filename)
        return int(max(years)) if years else 2025

    def extract_quarter_regex(self, text):
        months = re.findall(
            r'\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\b',
            text.lower()
        )
        qmap = {
            "jan": "Q1", "feb": "Q1", "mar": "Q1",
            "apr": "Q2", "may": "Q2", "jun": "Q2",
            "jul": "Q3", "aug": "Q3", "sep": "Q3",
            "oct": "Q4", "nov": "Q4", "dec": "Q4",
        }
        return qmap.get(months[0], "Q1") if months else "Q1"

    # ══════════════════════════════════════
    #  MASTER EXTRACTION
    # ══════════════════════════════════════
    async def extract_full(
        self, file_bytes, filename, category,
        text_from_processor=None
    ):
        """
        Returns LIST of records — one per line item.
        Each record has per-unit price.
        """
        print(f"\n  🔍 Extracting: {filename}")

        text = ""

        # LlamaCloud for PDFs
        from pathlib import Path
        if Path(filename).suffix.lower() == ".pdf":
            llama_text = await self.parse_pdf_with_llama(
                file_bytes, filename
            )
            if llama_text:
                text = llama_text

        if not text and text_from_processor:
            text = text_from_processor

        if not text:
            print(f"    ❌ No text extracted")
            return []

        # Claude extraction
        print(f"    🤖 Claude AI extraction...")
        extracted = self.extract_with_claude(text, filename, category)

        line_items = extracted.get("line_items", [])
        vendor     = extracted.get("vendor", "Unknown")
        cat        = extracted.get("category", "") or category
        year       = extracted.get("year", 0)
        quarter    = extracted.get("quarter", "Q1")

        # Fill gaps with regex
        if vendor == "Unknown":
            vendor = self.extract_vendor_regex(text, filename)
        if not year or year < 2020:
            year = self.extract_year_regex(text, filename)
        if not quarter:
            quarter = self.extract_quarter_regex(text)

        # If no line items from Claude use regex fallback
        if not line_items:
            total_price = self.extract_price_regex(text)
            services    = self.extract_services_regex(text)
            if total_price > 0:
                if services:
                    per_svc = total_price / len(services)
                    for svc in services:
                        line_items.append({
                            "service":         svc,
                            "unit_price":      round(per_svc, 2),
                            "quantity":        1,
                            "unit":            "per quote",
                            "total_line_price": total_price,
                            "description":     "",
                        })
                else:
                    line_items.append({
                        "service":         filename,
                        "unit_price":      total_price,
                        "quantity":        1,
                        "unit":            "per quote",
                        "total_line_price": total_price,
                        "description":     "",
                    })

        if not line_items:
            print(f"    ❌ No line items found")
            return []

        # Build one record per line item
        records = []
        for item in line_items:
            unit_price = item.get("unit_price", 0)
            if not unit_price or unit_price <= 0:
                continue

            record = {
                "cat":             cat,
                "vendor":          vendor,
                "file":            filename,
                "service":         item.get("service", "Unknown Service"),
                "unit_price":      round(float(unit_price), 2),
                "quantity":        item.get("quantity", 1),
                "unit":            item.get("unit", "per license"),
                "total_line_price": item.get("total_line_price", 0),
                "description":     item.get("description", ""),
                "year":            year,
                "quarter":         quarter,
                "currency":        extracted.get("currency", "USD"),
                "confidence":      extracted.get("confidence", "medium"),
                "source":          "sharepoint",
            }
            records.append(record)
            print(
                f"    ✅ {vendor} | "
                f"{item.get('service','')[:30]} | "
                f"${unit_price:,.2f}/{item.get('unit','unit')}"
            )

        print(f"    📦 {len(records)} line items extracted")
        return records

    def get_stats(self):
        return self.stats
