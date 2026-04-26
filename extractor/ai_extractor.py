# extractor/ai_extractor.py
# ═══════════════════════════════════════════════
# AI-powered extraction using:
# 1. LlamaCloud — agentic PDF parsing
# 2. Claude AI  — structured data extraction
# 3. Regex      — fast price fallback
# ═══════════════════════════════════════════════

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
            "llama_success": 0,
            "llama_failed":  0,
            "claude_success": 0,
            "claude_failed":  0,
            "regex_fallback": 0,
        }

    # ══════════════════════════════════════
    #  LLAMACLOUD — PDF PARSING
    # ══════════════════════════════════════
    async def parse_pdf_with_llama(self, file_bytes, filename):
        """
        Upload PDF to LlamaCloud and parse
        with agentic tier for best accuracy.
        Returns markdown text or empty string.
        """
        try:
            print(f"    📄 LlamaCloud parsing: {filename[:40]}...")

            # Upload file
            file_obj = await self.llama.files.create(
                file=(filename, file_bytes, "application/pdf"),
                purpose="parse"
            )

            # Parse with agentic tier
            result = await self.llama.parsing.parse(
                file_id=file_obj.id,
                tier=LLAMA_TIER,
                expand=LLAMA_EXPAND,
            )

            markdown = result.markdown_full or ""

            if markdown and len(markdown) > 100:
                self.stats["llama_success"] += 1
                print(
                    f"    ✅ LlamaCloud: "
                    f"{len(markdown):,} chars extracted"
                )
                return markdown
            else:
                self.stats["llama_failed"] += 1
                return ""

        except Exception as e:
            self.stats["llama_failed"] += 1
            print(f"    ⚠️  LlamaCloud failed: {e}")
            return ""

    # ══════════════════════════════════════
    #  CLAUDE — STRUCTURED EXTRACTION
    # ══════════════════════════════════════
    def extract_with_claude(self, text, filename, category):
        """
        Use Claude to extract structured pricing data
        from document text.
        Returns dict with vendor, price, services etc.
        """
        # Limit text to avoid token overflow
        text_truncated = text[:8000]

        # Build known vendors/services hints
        vendor_hints  = ", ".join(KNOWN_VENDORS[:15])
        service_hints = ", ".join(KNOWN_SERVICES[:20])

        prompt = f"""You are a specialist IT contract pricing analyst at PwC.
Extract structured pricing data from this vendor quote document.

DOCUMENT INFO:
Filename: {filename}
Category: {category}

KNOWN VENDORS (match if found):
{vendor_hints}

KNOWN SERVICES (match if found):
{service_hints}

DOCUMENT TEXT:
{text_truncated}

INSTRUCTIONS:
1. Find the TOTAL contract price (the final/grand total amount)
2. Identify the vendor company name
3. List all IT products and services mentioned
4. Determine the quote date (year and quarter)
5. Note the currency (assume USD if not specified)

Return ONLY a valid JSON object in this exact format:
{{
  "vendor": "exact vendor company name",
  "price": 71300,
  "services": [
    "Exact Service Name 1",
    "Exact Service Name 2"
  ],
  "year": 2025,
  "quarter": "Q1",
  "currency": "USD",
  "confidence": "high",
  "notes": "any important context"
}}

RULES:
- price: NUMBER only, no $ signs or commas
- price: use the TOTAL/GRAND TOTAL figure
- year: 4-digit year number
- quarter: Q1/Q2/Q3/Q4
- confidence: high (clear price), medium (estimated), low (unclear)
- If price not clearly found: use 0
- If vendor not found: use "Unknown"
- Return ONLY the JSON, no other text"""

        try:
            response = self.claude.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=CLAUDE_MAX_TOKENS,
                messages=[{"role": "user", "content": prompt}]
            )

            raw = response.content[0].text.strip()

            # Parse JSON from response
            result = self._parse_json_response(raw)

            if result and result.get("price", 0) > 0:
                self.stats["claude_success"] += 1
                return result
            else:
                self.stats["claude_failed"] += 1
                return self._empty_result()

        except Exception as e:
            self.stats["claude_failed"] += 1
            print(f"    ⚠️  Claude extraction failed: {e}")
            return self._empty_result()

    def _parse_json_response(self, raw_text):
        """Safely parse JSON from Claude response"""
        # Try direct parse first
        try:
            return json.loads(raw_text)
        except json.JSONDecodeError:
            pass

        # Extract JSON block
        json_match = re.search(
            r'\{[^{}]*\}',
            raw_text,
            re.DOTALL
        )
        if json_match:
            try:
                return json.loads(json_match.group())
            except json.JSONDecodeError:
                pass

        # Try fixing common issues
        try:
            fixed = raw_text
            fixed = re.sub(r',\s*}', '}', fixed)
            fixed = re.sub(r',\s*]', ']', fixed)
            return json.loads(fixed)
        except json.JSONDecodeError:
            pass

        return None

    def _empty_result(self):
        return {
            "vendor":     "Unknown",
            "price":      0,
            "services":   [],
            "year":       2025,
            "quarter":    "Q1",
            "currency":   "USD",
            "confidence": "low",
            "notes":      "",
        }

    # ══════════════════════════════════════
    #  REGEX FALLBACK — FAST PRICE EXTRACT
    # ══════════════════════════════════════
    def extract_price_regex(self, text):
        """
        Fast regex extraction as backup.
        Returns best price found or 0.
        """
        prices = []

        patterns = [
            # Total/Grand Total patterns
            r'(?:grand\s+total|total\s+amount|total\s+price'
            r'|total\s+cost|total\s+value|amount\s+due'
            r'|invoice\s+total)'
            r'[:\s$]*(?:USD\s*)?([\d,]+(?:\.\d{1,2})?)',

            # Currency patterns
            r'USD\s*([\d,]+(?:\.\d{1,2})?)',
            r'\$\s*([\d,]{4,}(?:\.\d{1,2})?)',

            # Generic price patterns
            r'(?:price|cost|quote|value|amount)'
            r'[:\s$]*(?:USD\s*)?([\d,]+(?:\.\d{1,2})?)',
        ]

        for pattern in patterns:
            for m in re.finditer(pattern, text, re.IGNORECASE):
                try:
                    n = float(
                        m.group(1)
                        .replace(',', '')
                        .strip()
                    )
                    if MIN_VALID_PRICE <= n <= MAX_VALID_PRICE:
                        prices.append(n)
                except (ValueError, IndexError):
                    continue

        if prices:
            self.stats["regex_fallback"] += 1
            # Return the most common large value
            # (likely the total, not a line item)
            prices.sort(reverse=True)
            return prices[0]

        return 0

    def extract_vendor_regex(self, text, filename):
        """Match known vendors from text or filename"""
        text_lower = text.lower()
        fname_lower = filename.lower()

        for vendor in KNOWN_VENDORS:
            v_lower = vendor.lower()
            if v_lower in text_lower or v_lower in fname_lower:
                return vendor

        return "Unknown"

    def extract_services_regex(self, text):
        """Match known services from text"""
        found   = []
        t_lower = text.lower()

        for service in KNOWN_SERVICES:
            if service.lower() in t_lower:
                found.append(service)

        return list(set(found))

    def extract_year_regex(self, text, filename):
        """Extract year from text or filename"""
        # Try text first
        years = re.findall(r'\b(202[0-9])\b', text + " " + filename)
        if years:
            return int(max(years))
        return 2025

    def extract_quarter_regex(self, text):
        """Determine quarter from dates in text"""
        months = re.findall(
            r'\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)'
            r'\w*\b',
            text.lower()
        )

        quarter_map = {
            "jan": "Q1", "feb": "Q1", "mar": "Q1",
            "apr": "Q2", "may": "Q2", "jun": "Q2",
            "jul": "Q3", "aug": "Q3", "sep": "Q3",
            "oct": "Q4", "nov": "Q4", "dec": "Q4",
        }

        if months:
            return quarter_map.get(months[0], "Q1")
        return "Q1"

    # ══════════════════════════════════════
    #  COMBINED EXTRACTION
    # ══════════════════════════════════════
    async def extract_full(
        self, file_bytes, filename, category, text_from_processor=None
    ):
        """
        Master extraction method.
        Tries LlamaCloud → Claude → Regex in order.
        Returns complete catalog record dict.
        """
        print(f"\n  🔍 Extracting: {filename}")

        text = ""

        # ── Step 1: LlamaCloud for PDFs ──
        from pathlib import Path
        if Path(filename).suffix.lower() == ".pdf":
            llama_text = await self.parse_pdf_with_llama(
                file_bytes, filename
            )
            if llama_text:
                text = llama_text
                print(
                    f"    ✅ Used LlamaCloud "
                    f"({len(text):,} chars)"
                )

        # ── Step 2: Use processor text as fallback ──
        if not text and text_from_processor:
            text = text_from_processor
            print(
                f"    ℹ️  Used file processor "
                f"({len(text):,} chars)"
            )

        if not text:
            print(f"    ❌ No text extracted")
            return None

        # ── Step 3: Claude extraction ──
        print(f"    🤖 Claude AI extraction...")
        extracted = self.extract_with_claude(text, filename, category)

        # ── Step 4: Fill gaps with regex ──
        price = extracted.get("price", 0)
        if not price or price <= 0:
            price = self.extract_price_regex(text)
            print(f"    🔢 Regex price fallback: ${price:,.0f}")

        vendor = extracted.get("vendor", "Unknown")
        if vendor == "Unknown":
            vendor = self.extract_vendor_regex(text, filename)

        services = extracted.get("services", [])
        if not services:
            services = self.extract_services_regex(text)

        year    = extracted.get("year", 0)
        if not year or year < 2020:
            year = self.extract_year_regex(text, filename)

        quarter = extracted.get("quarter", "Q1")
        if not quarter:
            quarter = self.extract_quarter_regex(text)

        # ── Step 5: Validate ──
        if price <= 0:
            print(f"    ❌ No valid price found — skipping")
            return None

        record = {
            "cat":        category,
            "vendor":     vendor,
            "file":       filename,
            "services":   services,
            "price":      round(price, 2),
            "year":       year,
            "quarter":    quarter,
            "currency":   extracted.get("currency", "USD"),
            "confidence": extracted.get("confidence", "medium"),
            "source":     "sharepoint",
            "notes":      extracted.get("notes", ""),
        }

        print(
            f"    ✅ {vendor} | "
            f"${price:,.0f} | "
            f"{len(services)} services | "
            f"{year} {quarter}"
        )

        return record

    def get_stats(self):
        return self.stats
