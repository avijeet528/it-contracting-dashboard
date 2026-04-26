# extractor/main.py
# ═══════════════════════════════════════════════
# Master orchestrator — runs the full pipeline:
# SharePoint → Extract → Build Catalog → GitHub
# ═══════════════════════════════════════════════

import asyncio
import json
import time
import sys
from datetime import datetime
from pathlib import Path

from sharepoint_connector import SharePointConnector
from file_processor        import FileProcessor
from ai_extractor          import AIExtractor
from catalog_builder       import CatalogBuilder
from github_pusher         import GitHubPusher
from config import (
    DELAY_BETWEEN_FILES,
    DELAY_BETWEEN_FOLDERS,
    OUTPUT_FILE,
)


# ══════════════════════════════════════════════
#  PROCESS ONE FILE
# ══════════════════════════════════════════════
async def process_single_file(
    file_info, category,
    sp_connector, file_processor, ai_extractor, catalog_builder
):
    """
    Full pipeline for a single file:
    Download → Extract Text → AI Extract → Add to Catalog
    """
    filename = file_info.get("name", "unknown")

    try:
        # Step 1 — Download from SharePoint
        print(f"\n  📥 Downloading: {filename[:50]}")
        file_bytes = sp_connector.download_file(file_info)

        if not file_bytes:
            print(f"  ⚠️  Empty file — skipping")
            catalog_builder.add_error(
                filename, category, "Empty file downloaded"
            )
            return False

        print(
            f"  📦 Downloaded: "
            f"{len(file_bytes):,} bytes"
        )

        # Step 2 — Extract text (for non-PDF or fallback)
        text, method = file_processor.extract_text(
            file_bytes, filename
        )

        if text:
            print(
                f"  📝 Text extracted via: {method} "
                f"({len(text):,} chars)"
            )

        # Step 3 — AI extraction
        record = await ai_extractor.extract_full(
            file_bytes=file_bytes,
            filename=filename,
            category=category,
            text_from_processor=text,
        )

        # Step 4 — Add to catalog
        if record:
            added = catalog_builder.add_record(record)
            if added:
                print(
                    f"  ✅ Added: "
                    f"{record['vendor']} | "
                    f"${record['price']:,.0f} | "
                    f"{len(record.get('services',[]))} services"
                )
                return True
            else:
                print(f"  ⏭️  Skipped (validation/duplicate)")
                return False
        else:
            print(f"  ❌ No data extracted")
            catalog_builder.add_error(
                filename, category, "No valid data extracted"
            )
            return False

    except Exception as e:
        error_msg = str(e)
        print(f"  ❌ Error processing {filename}: {error_msg}")
        catalog_builder.add_error(filename, category, error_msg)
        return False


# ══════════════════════════════════════════════
#  MAIN PIPELINE
# ══════════════════════════════════════════════
async def run_extraction():
    """Full extraction pipeline"""

    start_time = time.time()

    print("=" * 65)
    print("  IT Contracting Dashboard — SharePoint Extractor")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    # ── Initialise components ──
    sp_connector    = SharePointConnector()
    file_processor  = FileProcessor()
    ai_extractor    = AIExtractor()
    catalog_builder = CatalogBuilder()
    github_pusher   = GitHubPusher()

    # ── Step 1: Connect to SharePoint ──
    print("\n🔗 STEP 1: Connecting to SharePoint...")
    try:
        sp_connector.connect()
    except Exception as e:
        print(f"❌ SharePoint connection failed: {e}")
        print(
            "\n💡 Check your Azure App Registration credentials in .env"
        )
        sys.exit(1)

    # ── Step 2: List all files ──
    print("\n📋 STEP 2: Listing files in SharePoint...")
    all_files = sp_connector.list_all_category_files()

    total_files = sum(len(f) for f in all_files.values())
    print(f"\n📊 Total files found: {total_files}")
    for cat, files in all_files.items():
        print(f"   {cat}: {len(files)} files")

    if total_files == 0:
        print("❌ No files found — check folder paths in config.py")
        sys.exit(1)

    # ── Step 3: Process each file ──
    print(f"\n⚙️  STEP 3: Processing {total_files} files...")

    processed = 0
    successful = 0

    for category, files in all_files.items():
        if not files:
            continue

        print(f"\n{'─'*65}")
        print(f"📁 CATEGORY: {category} ({len(files)} files)")
        print(f"{'─'*65}")

        for i, file_info in enumerate(files, 1):
            print(
                f"\n[{processed+1}/{total_files}] "
                f"File {i}/{len(files)}"
            )

            result = await process_single_file(
                file_info, category,
                sp_connector, file_processor,
                ai_extractor, catalog_builder
            )

            if result:
                successful += 1

            processed += 1

            # Rate limiting
            if i < len(files):
                time.sleep(DELAY_BETWEEN_FILES)

        # Delay between folders
        time.sleep(DELAY_BETWEEN_FOLDERS)

    # ── Step 4: Save catalog ──
    print(f"\n💾 STEP 4: Saving catalog...")
    catalog_builder.save()

    # ── Step 5: Print summary ──
    catalog_builder.print_summary()

    elapsed = round(time.time() - start_time, 1)
    print(f"\n⏱️  Total time: {elapsed}s")

    # Print AI stats
    ai_stats = ai_extractor.get_stats()
    print(f"\n🤖 AI Usage:")
    for k, v in ai_stats.items():
        print(f"   {k}: {v}")

    # ── Step 6: Push to GitHub ──
    print(f"\n📤 STEP 5: Pushing to GitHub...")
    try:
        github_pusher.test_connection()
        success = github_pusher.push_catalog(OUTPUT_FILE)

        if success:
            commit = github_pusher.get_latest_commit()
            if commit:
                print(
                    f"   Latest commit: "
                    f"{commit.get('sha')} — "
                    f"{commit.get('message','')[:50]}"
                )
    except Exception as e:
        print(f"⚠️  GitHub push failed: {e}")
        print(
            f"   catalog_data.json saved locally — "
            f"push manually"
        )

    print(f"\n{'='*65}")
    print(
        f"🎉 DONE — "
        f"{successful}/{total_files} files processed successfully"
    )
    print(f"{'='*65}\n")

    return catalog_builder.records


# ══════════════════════════════════════════════
#  ENTRY POINTS
# ══════════════════════════════════════════════
if __name__ == "__main__":

    # Parse command line arguments
    args = sys.argv[1:]

    if "--test" in args:
        # Test connections only
        print("🧪 Running connection tests...\n")
        sp = SharePointConnector()
        sp.test_connection()
        gh = GitHubPusher()
        gh.test_connection()

    elif "--push-only" in args:
        # Just push existing catalog_data.json
        if not Path(OUTPUT_FILE).exists():
            print(f"❌ {OUTPUT_FILE} not found — run full extraction first")
            sys.exit(1)
        gh = GitHubPusher()
        gh.push_catalog(OUTPUT_FILE)

    else:
        # Full extraction pipeline
        asyncio.run(run_extraction())
