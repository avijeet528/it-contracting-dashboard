# extractor/main.py — updated for per-license records

import asyncio
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


async def process_single_file(
    file_info, category,
    sp_connector, file_processor,
    ai_extractor, catalog_builder
):
    filename = file_info.get("name", "unknown")

    try:
        print(f"\n  📥 Downloading: {filename[:50]}")
        file_bytes = sp_connector.download_file(file_info)

        if not file_bytes:
            catalog_builder.add_error(
                filename, category, "Empty file"
            )
            return 0

        text, method = file_processor.extract_text(
            file_bytes, filename
        )
        if text:
            print(f"  📝 {method} ({len(text):,} chars)")

        # Returns LIST of records now
        records = await ai_extractor.extract_full(
            file_bytes=file_bytes,
            filename=filename,
            category=category,
            text_from_processor=text,
        )

        added = catalog_builder.add_records(records)
        return added

    except Exception as e:
        print(f"  ❌ Error: {filename}: {e}")
        catalog_builder.add_error(filename, category, str(e))
        return 0


async def run_extraction():
    start_time = time.time()

    print("=" * 65)
    print("  IT Contracting Dashboard — SharePoint Extractor")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    sp_connector    = SharePointConnector()
    file_processor  = FileProcessor()
    ai_extractor    = AIExtractor()
    catalog_builder = CatalogBuilder()
    github_pusher   = GitHubPusher()

    print("\n🔗 STEP 1: Connecting to SharePoint...")
    try:
        sp_connector.connect()
    except Exception as e:
        print(f"❌ SharePoint connection failed: {e}")
        sys.exit(1)

    print("\n📋 STEP 2: Listing files...")
    all_files   = sp_connector.list_all_category_files()
    total_files = sum(len(f) for f in all_files.values())

    print(f"\n📊 Total files: {total_files}")
    for cat, files in all_files.items():
        print(f"   {cat}: {len(files)} files")

    if total_files == 0:
        print("❌ No files found")
        sys.exit(1)

    print(f"\n⚙️  STEP 3: Processing {total_files} files...")

    processed  = 0
    total_items = 0

    for category, files in all_files.items():
        if not files:
            continue
        print(f"\n{'─'*65}")
        print(f"📁 {category} ({len(files)} files)")
        print(f"{'─'*65}")

        for i, file_info in enumerate(files, 1):
            print(f"\n[{processed+1}/{total_files}]")
            added = await process_single_file(
                file_info, category,
                sp_connector, file_processor,
                ai_extractor, catalog_builder
            )
            total_items += added
            processed   += 1
            if i < len(files):
                time.sleep(DELAY_BETWEEN_FILES)

        time.sleep(DELAY_BETWEEN_FOLDERS)

    print(f"\n💾 STEP 4: Saving...")
    catalog_builder.save()
    catalog_builder.print_summary()

    elapsed = round(time.time() - start_time, 1)
    print(f"\n⏱️  Total time: {elapsed}s")
    print(f"📦 Total line items: {total_items}")

    print(f"\n📤 STEP 5: Pushing to GitHub...")
    try:
        github_pusher.test_connection()
        github_pusher.push_catalog(OUTPUT_FILE)
    except Exception as e:
        print(f"⚠️  GitHub push failed: {e}")

    print(f"\n{'='*65}")
    print(f"🎉 DONE")
    print(f"{'='*65}\n")
    return catalog_builder.records


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--test" in args:
        sp = SharePointConnector()
        sp.test_connection()
        gh = GitHubPusher()
        gh.test_connection()
    elif "--push-only" in args:
        if not Path(OUTPUT_FILE).exists():
            print(f"❌ {OUTPUT_FILE} not found")
            sys.exit(1)
        gh = GitHubPusher()
        gh.push_catalog(OUTPUT_FILE)
    else:
        asyncio.run(run_extraction())
