import os
import re
import json
import time
import fitz
import pandas as pd
from PIL import Image
from io import BytesIO
from langdetect import detect
from collections import Counter
import multiprocessing
from pathlib import Path
from data_io import load_data, save_data, load_last_folder, save_last_folder

# === CONFIGURATION ===
output_image_dir = "preview_images"
output_csv = "Books_Data.csv"
error_log_path = "pdf_errors.log"
cache_file = "last_scanned.json"

os.makedirs(output_image_dir, exist_ok=True)

excluded_keywords = {
    'copyright', 'introduction', 'author', 'summary', 'glossary', 'disclaimer',
    'page', 'note', 'contents', 'table', 'index', 'preface', 'authors',
    'foreword', 'appendix', 'the', 'this', 'that', 'about', 'than', 'from',
    'publisher', 'thanks', 'neither', 'through', 'extracted'
}


# === UTILITY FUNCTIONS ===

def is_english(word):
    try:
        return detect(word) == 'en'
    except:
        return False


def clean_text(text):
    text = re.sub(r'[^A-Za-z\s]', '', text)
    words = text.split()
    return ' '.join([w for w in words if len(w) > 2 and is_english(w)]).title()


def log_error(pdf_path, message):
    with open(error_log_path, 'a', encoding='utf-8') as f:
        f.write(f"{pdf_path} - {message}\n")


# === EXTRACTION FUNCTIONS ===

def extract_keywords(pdf_path, max_pages=15, top_n=15):
    try:
        with fitz.open(pdf_path) as doc:
            full_text = ''.join(doc.load_page(i).get_text() for i in range(min(max_pages, len(doc))))
        words = re.sub(r'[^a-zA-Z\s]', '', full_text).lower().split()
        filtered = [w for w in words if len(w) > 5 and w not in excluded_keywords and is_english(w)]
        return ', '.join([w for w, _ in Counter(filtered).most_common(top_n)]).title()
    except Exception as e:
        log_error(pdf_path, f"Keyword extraction failed: {e}")
        return ""


def extract_bookmarks(pdf_path):
    try:
        with fitz.open(pdf_path) as doc:
            toc = doc.get_toc()
        return (True, [item[1].upper() for item in toc]) if toc else (False, [])
    except Exception as e:
        log_error(pdf_path, f"Bookmark extraction failed: {e}")
        return False, []


def extract_fallback_bookmarks(pdf_path, max_pages=10):
    bookmarks = []
    try:
        with fitz.open(pdf_path) as doc:
            for i in range(min(max_pages, len(doc))):
                text = doc.load_page(i).get_text()
                lines = [clean_text(line) for line in text.split('\n')]
                bookmarks += [line for line in lines if line and ("Content" in line or len(line.split()) > 2)]
    except Exception as e:
        log_error(pdf_path, f"Fallback TOC failed: {e}")
    return bookmarks


def extract_first_page_image(pdf_path, output_dir, index, zoom=1.2, quality=60):
    try:
        with fitz.open(pdf_path) as doc:
            pix = doc.load_page(0).get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False)
        img = Image.open(BytesIO(pix.tobytes("ppm")))
        output_path = os.path.join(output_dir, f"{index:04d}.jpg")
        img.save(output_path, "JPEG", quality=quality, optimize=True)
        return output_path
    except Exception as e:
        log_error(pdf_path, f"Image generation failed: {e}")
        return ""


def extract_metadata(doc, pdf_path):
    try:
        author = doc.metadata.get('author') or 'Not embedded in this file'
        isbn = 'Missing from PDF metadata'
        year = 'Missing from PDF metadata'

        for i in range(min(5, len(doc))):
            text = doc.load_page(i).get_text()

            # Extract year
            year_match = re.search(r'\b(?:published|copyright|©)?\s*((19|20)\d{2})\b', text, re.IGNORECASE)
            if year_match:
                year = year_match.group(1)

            # Extract ISBN
            isbn_match = re.search(
                r'\b(?:ISBN(?:-1[03])?:?\s*)?((?:97[89][-\s]?)?\d{1,5}[-\s]?\d{1,7}[-\s]?\d{1,7}[-\s]?[\dxX])\b',
                text,
                re.IGNORECASE
            )

            if isbn_match:
                raw_isbn = isbn_match.group(1)
                cleaned_isbn = re.sub(r'[-\s]', '', raw_isbn)
                if len(cleaned_isbn) >= 10 and not re.fullmatch(r'(19|20)\d{2}', cleaned_isbn):
                    isbn = cleaned_isbn
                    break

        return author.title(), year, isbn
    except Exception as e:
        log_error(pdf_path, f"Metadata fallback failed: {e}")
        return author.title(), 'Missing from PDF metadata', 'Missing from PDF metadata'


# === CORE PROCESSING ===

def process_pdf(job):
    index, full_path, rel_path = job

    if not Path(full_path).exists() or Path(full_path).suffix.lower() != '.pdf':
        return None

    try:
        with fitz.open(full_path) as doc:
            pages = len(doc)
            author, year, isbn = extract_metadata(doc, full_path)
    except Exception as e:
        log_error(full_path, f"PDF open failed: {e}")
        return None

    has_bm, bookmarks = extract_bookmarks(full_path)
    if not has_bm:
        bookmarks = extract_fallback_bookmarks(full_path)

    bookmarks_clean = '; '.join(bookmarks)
    image_path = extract_first_page_image(full_path, output_image_dir, index)
    keywords = extract_keywords(full_path)

    return {
        'Index': index,
        'File Name': os.path.basename(full_path),
        'Year': year,
        'ISBN': isbn,
        'Page Count': int(pages),
        'Author': author,
        'Section': Path(full_path).parent.name,
        'Absolute Path': full_path,
        'Has Bookmarks': has_bm,
        'Table of Contents': bookmarks_clean,
        'Preview Image': image_path,
        'Read Status': 'Unread',
        'Keywords': keywords,
        'Description': ''
    }


# === CACHING & FILE DISCOVERY ===

def load_cache():
    if os.path.exists(cache_file):
        with open(cache_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(cache, f)


def find_pdfs_to_process(root, cache):
    jobs = []
    index = 1
    for dirpath, _, files in os.walk(root):
        for file in sorted(files):
            if file.lower().endswith('.pdf'):
                full_path = os.path.join(dirpath, file)
                rel_path = os.path.relpath(full_path, root)
                jobs.append((index, full_path, rel_path))
                index += 1
    return jobs


# === MAIN PIPELINE ===

def start_scan(folder, on_progress=None):
    unlock_after = 1
    cache = load_cache()
    jobs = find_pdfs_to_process(folder, cache)
    total_jobs = len(jobs)
    print(f"📁 Found {total_jobs} new/updated PDFs.")

    processed = []
    new_cache = {}

    for i, job in enumerate(jobs, 1):
        result = process_pdf(job)
        if result:
            processed.append(result)
            if on_progress and (i % unlock_after == 0 or i == total_jobs):
                on_progress(i, total_jobs)

        rel_path = job[2]
        stat = os.stat(job[1])
        new_cache[rel_path] = f"{job[1].split(os.sep)[-1]}_{stat.st_size}_{int(stat.st_mtime)}"

    df = pd.DataFrame(processed)

    if not df.empty:
        save_cache(new_cache)
        export_to_csv(df)
        save_data(df, folder)
        return df

    return pd.DataFrame()


def export_to_csv(results, path=output_csv):
    df = pd.DataFrame(results)
    df['Page Count'] = df['Page Count'].astype(int)

    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].astype(str).apply(lambda x: x.title())

    df.to_csv(path, index=False)
    print(f"✅ Exported {len(df)} records to {path}")


# === ENTRY POINT ===

def main():
    print("Main function started")
    time.sleep(5)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
