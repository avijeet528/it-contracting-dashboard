# extractor/catalog_builder.py
# ═══════════════════════════════════════════════
# Builds, validates, deduplicates and
# enriches the final catalog_data.json
# ═══════════════════════════════════════════════

import json
import re
from datetime import datetime
from config import (
    OUTPUT_FILE,
    ERROR_LOG_FILE,
    PROGRESS_FILE,
    MIN_VALID_PRICE,
    MAX_VALID_PRICE,
)


class CatalogBuilder:

    def __init__(self):
        self.records    = []
        self.errors     = []
        self.skipped    = []
        self.duplicates = []

    # ══════════════════════════════════════
    #  ADD RECORDS
    # ══════════════════════════════════════
    def add_record(self, record):
        """Add and validate a single record"""
        if not record:
            return False

        # Validate
        issues = self._validate(record)
        if issues:
            self.skipped.append({
                "file":   record.get("file", "unknown"),
                "issues": issues,
            })
            return False

        # Clean
        cleaned = self._clean(record)

        # Check duplicate
        if self._is_duplicate(cleaned):
            self.duplicates.append(cleaned["file"])
            return False

        self.records.append(cleaned)
        return True

    def add_error(self, filename, category, error_msg):
        """Log a processing error"""
        self.errors.append({
            "file":     filename,
            "category": category,
            "error":    str(error_msg),
            "time":     datetime.now().isoformat(),
        })

    # ══════════════════════════════════════
    #  VALIDATION
    # ══════════════════════════════════════
    def _validate(self, record):
        """
        Validate a record.
        Returns list of issues (empty = valid).
        """
        issues = []

        # Required fields
        if not record.get("cat"):
            issues.append("missing category")

        if not record.get("file"):
            issues.append("missing filename")

        # Price validation
        price = record.get("price", 0)
        try:
            price = float(price)
        except (TypeError, ValueError):
            issues.append(f"invalid price: {price}")
            return issues

        if price <= 0:
            issues.append("price is zero or negative")
        elif price < MIN_VALID_PRICE:
            issues.append(
                f"price too low: ${price:,.0f} "
                f"(min ${MIN_VALID_PRICE:,})"
            )
        elif price > MAX_VALID_PRICE:
            issues.append(
                f"price too high: ${price:,.0f} "
                f"(max ${MAX_VALID_PRICE:,})"
            )

        # Year validation
        year = record.get("year", 0)
        if year and (year < 2018 or year > 2030):
            issues.append(f"suspicious year: {year}")

        return issues

    # ══════════════════════════════════════
    #  CLEANING
    # ══════════════════════════════════════
    def _clean(self, record):
        """Clean and normalise a record"""
        cleaned = dict(record)

        # Clean vendor name
        vendor = str(cleaned.get("vendor", "Unknown")).strip()
        vendor = self._normalise_vendor(vendor)
        cleaned["vendor"] = vendor

        # Clean services list
        services = cleaned.get("services", [])
        if isinstance(services, str):
            services = [s.strip() for s in services.split(",")]
        cleaned["services"] = [
            s.strip()
            for s in services
            if s and str(s).strip() and len(str(s).strip()) > 2
        ]

        # Ensure numeric price
        cleaned["price"] = round(float(cleaned.get("price", 0)), 2)

        # Ensure year is int
        try:
            cleaned["year"] = int(cleaned.get("year", 2025))
        except (TypeError, ValueError):
            cleaned["year"] = 2025

        # Normalise quarter
        q = str(cleaned.get("quarter", "Q1")).upper().strip()
        if q not in ["Q1", "Q2", "Q3", "Q4"]:
            q = "Q1"
        cleaned["quarter"] = q

        # Ensure category string
        cleaned["cat"] = str(cleaned.get("cat", "")).strip()

        # Remove internal-only fields
        for field in ["source", "confidence", "notes"]:
            cleaned.pop(field, None)

        return cleaned

    def _normalise_vendor(self, vendor):
        """Normalise vendor names to canonical form"""
        normalise_map = {
            "ntt":         "NTT Data",
            "nttdata":     "NTT Data",
            "ntt data":    "NTT Data",
            "ntt docomo":  "NTT DOCOMO",
            "trendmicro":  "TrendMicro",
            "trend micro": "TrendMicro",
            "knowbe4":     "KnowBe4",
            "know be4":    "KnowBe4",
            "shi":         "SHI",
            "pc connection": "PC Connection",
            "cdw":         "CDW",
            "equinix":     "Equinix",
            "quest":       "Quest",
            "servicenow":  "ServiceNow",
            "service now": "ServiceNow",
            "microsoft":   "Microsoft",
            "proquire":    "Proquire LLC",
            "ricoh":       "Ricoh",
            "honeywell":   "Honeywell",
        }

        v_lower = vendor.lower().strip()
        for key, canonical in normalise_map.items():
            if key in v_lower:
                return canonical

        return vendor

    # ══════════════════════════════════════
    #  DEDUPLICATION
    # ══════════════════════════════════════
    def _is_duplicate(self, record):
        """
        Check if a record with same file + price
        already exists
        """
        for existing in self.records:
            if (
                existing.get("file")  == record.get("file")
                and existing.get("price") == record.get("price")
            ):
                return True
        return False

    def deduplicate(self):
        """Remove duplicate records from the full list"""
        seen   = set()
        unique = []

        for r in self.records:
            key = f"{r.get('file','')}_{r.get('price',0)}"
            if key not in seen:
                seen.add(key)
                unique.append(r)

        removed = len(self.records) - len(unique)
        if removed > 0:
            print(f"   🔄 Removed {removed} duplicate records")

        self.records = unique

    # ══════════════════════════════════════
    #  ENRICHMENT
    # ══════════════════════════════════════
    def enrich(self):
        """Add computed fields to each record"""
        for r in self.records:
            cat  = r.get("cat", "")
            vendor = r.get("vendor", "")
            price  = r.get("price", 0)

            # Add category average comparison
            cat_prices = [
                x["price"] for x in self.records
                if x.get("cat") == cat and x.get("price", 0) > 0
            ]
            if cat_prices:
                cat_avg = sum(cat_prices) / len(cat_prices)
                r["pct_vs_cat_avg"] = round(
                    (price - cat_avg) / cat_avg * 100, 1
                ) if cat_avg > 0 else 0

        return self.records

    # ══════════════════════════════════════
    #  SAVE
    # ══════════════════════════════════════
    def save(self):
        """Save catalog_data.json and error log"""

        # Deduplicate before saving
        self.deduplicate()

        # Save main catalog
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(self.records, f, indent=2, ensure_ascii=False)

        print(f"\n💾 Saved {len(self.records)} records → {OUTPUT_FILE}")

        # Save errors
        if self.errors:
            with open(ERROR_LOG_FILE, "w", encoding="utf-8") as f:
                json.dump(self.errors, f, indent=2)
            print(
                f"⚠️  Saved {len(self.errors)} errors → {ERROR_LOG_FILE}"
            )

        # Save progress/stats
        stats = self.get_stats()
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2)

        return OUTPUT_FILE

    # ══════════════════════════════════════
    #  STATS & REPORTING
    # ══════════════════════════════════════
    def get_stats(self):
        """Return full extraction statistics"""
        by_category = {}
        by_vendor   = {}

        for r in self.records:
            cat = r.get("cat", "Unknown")
            ven = r.get("vendor", "Unknown")
            p   = r.get("price", 0)

            if cat not in by_category:
                by_category[cat] = {"count": 0, "total": 0, "min": float("inf"), "max": 0}
            by_category[cat]["count"] += 1
            by_category[cat]["total"] += p
            by_category[cat]["min"]    = min(by_category[cat]["min"], p)
            by_category[cat]["max"]    = max(by_category[cat]["max"], p)

            if ven not in by_vendor:
                by_vendor[ven] = {"count": 0, "total": 0}
            by_vendor[ven]["count"] += 1
            by_vendor[ven]["total"] += p

        return {
            "total_records": len(self.records),
            "total_errors":  len(self.errors),
            "total_skipped": len(self.skipped),
            "duplicates_removed": len(self.duplicates),
            "by_category":   by_category,
            "by_vendor":     by_vendor,
            "generated_at":  datetime.now().isoformat(),
        }

    def print_summary(self):
        """Print human-readable summary"""
        stats = self.get_stats()

        print(f"\n{'='*60}")
        print(f"📊 EXTRACTION SUMMARY")
        print(f"{'='*60}")
        print(f"✅ Records extracted:   {stats['total_records']}")
        print(f"❌ Errors:              {stats['total_errors']}")
        print(f"⏭️  Skipped:             {stats['total_skipped']}")
        print(f"🔄 Duplicates removed:  {stats['duplicates_removed']}")

        print(f"\n📁 By Category:")
        for cat, data in stats["by_category"].items():
            avg = data["total"] / data["count"] if data["count"] else 0
            print(
                f"  {cat[:30]:<30} "
                f"{data['count']:>3} records | "
                f"Avg: ${avg:>10,.0f}"
            )

        print(f"\n🏢 By Vendor:")
        for ven, data in sorted(
            stats["by_vendor"].items(),
            key=lambda x: x[1]["count"],
            reverse=True
        )[:10]:
            print(
                f"  {ven[:25]:<25} "
                f"{data['count']:>3} records"
            )
