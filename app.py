#!/usr/bin/env python3
"""
Flipkart Label Sorter
=====================
Sorts Flipkart shipping label PDFs by product/pack-size,
crops to label-only (no invoice), and produces per-account
sorted-labels PDF + summary PDF.

Run:  python3 app.py
Open: http://localhost:5050
"""

import os, re, io, json, tempfile, traceback
from collections import defaultdict
from pathlib import Path

from flask import Flask, request, jsonify, send_file, render_template, Response
import pdfplumber
from pypdf import PdfReader, PdfWriter
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from reportlab.lib.units import mm

app = Flask(__name__)
UPLOAD_FOLDER = tempfile.mkdtemp()

# ─────────────────────────────────────────────
# HTML FRONTEND
# ─────────────────────────────────────────────

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

ACCOUNT_PATTERNS = {
    "EONSPARK": "EONSPARK",
    "REFRESHWAVE": "REFRESHWAVE",
    "CUTEST CLUB": "CUTEST CLUB",
    "HELLOHI": "HELLOHI",
}

def detect_account(text):
    """Detect seller account from label text."""
    text_upper = text.upper()
    for key, name in ACCOUNT_PATTERNS.items():
        if key in text_upper:
            return name
    # Fallback: extract "Sold By:XXXX" pattern
    m = re.search(r'Sold By[:\s]+([A-Z][A-Za-z0-9 &\(\)]+?)(?:,|\n|PRIVATE|LIMITED)', text, re.IGNORECASE)
    if m:
        return m.group(1).strip().upper()[:40]
    return "UNKNOWN_ACCOUNT"

def extract_skus_from_page(text):
    """
    Extract list of (sku_name, qty) from a label page.
    Returns list of dicts.
    """
    lines = text.split('\n')
    skus = []
    capture = False
    for line in lines:
        if 'SKU ID | Description' in line or 'SKU ID|Description' in line:
            capture = True
            continue
        if capture:
            # Line like: "1careu Scalp Massager Pack 1 | careu silicone hair scalp 1"
            m = re.match(r'^\d+\s*(.+?)\s*\|\s*.+?(\d+)\s*$', line)
            if m:
                skus.append({'sku': m.group(1).strip(), 'qty': int(m.group(2))})
            elif re.match(r'^FMP[A-Z0-9]', line) or re.match(r'^AWB', line) or re.match(r'^Tax Invoice', line):
                capture = False
    return skus

def load_sku_master(xlsx_bytes):
    """
    Parse xlsx (multi-sheet) into:
      { SHEET_NAME_UPPER: { sku: {product, pack_size, pack_raw} } }
    Each sheet = one seller account. Sheet name matched against detected
    account name from labels via get_account_master().
    """
    import openpyxl
    master = {}
    wb = openpyxl.load_workbook(io.BytesIO(xlsx_bytes), data_only=True)
    for sheet in wb.worksheets:
        account_key = sheet.title.strip().upper()
        rows = list(sheet.iter_rows(values_only=True))
        if not rows:
            continue
        header = None
        data_start = 0
        for i, row in enumerate(rows):
            norm = [str(c).strip().lower() if c else '' for c in row]
            if any(x in norm for x in ['sku', 'sku id', 'skuid']):
                header = norm
                data_start = i + 1
                break
        if not header:
            continue
        def col(names, start_after=None):
            # Exact match first
            for n in names:
                for i, h in enumerate(header):
                    if start_after is not None and i <= start_after:
                        continue
                    if h == n:
                        return i
            # Prefix match (e.g. 'productname 1' matches 'productname')
            for n in names:
                for i, h in enumerate(header):
                    if start_after is not None and i <= start_after:
                        continue
                    if h.startswith(n):
                        return i
            return None
        sku_col   = col(['sku', 'sku id', 'skuid'])
        prod1_col = col(['productname 1', 'productname', 'product name', 'product'])
        pack1_col = col(['packsize', 'pack size', 'pack'])
        # ProductName 2 and its PackSize must come AFTER prod1_col in the header
        prod2_col = col(['productname 2', 'productname'], start_after=prod1_col) if prod1_col is not None else None
        pack2_col = col(['packsize', 'pack size', 'pack'], start_after=pack1_col) if pack1_col is not None else None
        if sku_col is None:
            continue
        account_skus = {}
        for row in rows[data_start:]:
            sku      = str(row[sku_col]).strip()    if row[sku_col]                              else ''
            product  = str(row[prod1_col]).strip()  if prod1_col is not None and row[prod1_col] else ''
            pack_raw = str(row[pack1_col]).strip()  if pack1_col is not None and row[pack1_col] else '0'
            prod2    = str(row[prod2_col]).strip()  if prod2_col is not None and row[prod2_col] else ''
            pack2_raw= str(row[pack2_col]).strip()  if pack2_col is not None and row[pack2_col] else '0'
            if sku and sku.lower() not in ('none', ''):
                try:
                    pack_num = int(float(pack_raw)) if pack_raw else 0
                except:
                    pack_num = 0
                try:
                    pack2_num = int(float(pack2_raw)) if pack2_raw else 0
                except:
                    pack2_num = 0
                entry = {'product': product, 'pack_size': pack_num, 'pack_raw': pack_raw,
                         'product2': prod2, 'pack_size2': pack2_num}
                account_skus[sku] = entry
        master[account_key] = account_skus
    return master


def get_account_master(master, account_name):
    """
    Fuzzy-match detected account name against xlsx sheet names.
    e.g. detected "REFRESHWAVE" matches sheet "Refreshwave".
    Returns the SKU dict for that account, or empty dict if no match.
    """
    account_upper = account_name.upper()
    if account_upper in master:
        return master[account_upper]
    for sheet_key in master:
        if sheet_key in account_upper or account_upper in sheet_key:
            return master[sheet_key]
    return {}

def crop_page_top_half(page):
    """Crop PDF page to top half (label only, removes invoice)."""
    mb = page.mediabox
    mid_y = (mb.top + mb.bottom) / 2
    page.mediabox.bottom = mid_y
    page.cropbox.bottom = mid_y
    return page

def is_mixed_order(skus_on_page, master):
    """
    Returns True if this label is a mixed order:
    - Multiple SKUs on one label regardless of whether same/different product
    - Or 1 SKU but qty > 1 with different tracking (handled at merge level)
    """
    if len(skus_on_page) > 1:
        return True
    return False

def classify_pages(pages_data, master):
    """
    pages_data: list of {page_idx, skus: [{sku, qty}], text}
    Returns: (normal_pages, dual_pages, mixed_pages, unknown_pages)
    - normal : single SKU, single product
    - dual   : single SKU, but master says it contains 2 products
    - mixed  : multiple SKU entries on one label
    - unknown: SKU not in master
    """
    normal = []
    dual = []
    mixed = []
    unknown = []

    for pd in pages_data:
        skus = pd['skus']

        if not skus:
            unknown.append(pd)
            continue

        # Multiple SKU lines on label
        if len(skus) > 1:
            # Check if all SKU lines resolve to the same product
            resolved_products = set()
            total_qty = 0
            all_known = True
            for s in skus:
                info = master.get(s['sku'])
                if info and info['product']:
                    resolved_products.add((info['product'], info['pack_size']))
                    total_qty += s['qty']
                else:
                    all_known = False
                    break
            if all_known and len(resolved_products) == 1:
                # All lines are the same product — treat as normal order
                # Effective pack size = base pack_size × total number of units
                product, pack_size = list(resolved_products)[0]
                pd['primary_product'] = product
                pd['pack_size'] = pack_size * total_qty  # e.g. Pack 1 × 2 units = Pack 2
                pd['effective_qty'] = total_qty
                normal.append(pd)
            else:
                mixed.append(pd)
            continue

        # Single SKU — look it up
        sku_name = skus[0]['sku']
        info = master.get(sku_name)
        if not info:
            pd['primary_product'] = sku_name
            pd['pack_size'] = 0
            unknown.append(pd)
            continue

        pd['primary_product'] = info['product']
        pd['pack_size']        = info['pack_size']

        # Check if this SKU is a dual-product bundle
        if info.get('product2'):
            pd['product2']   = info['product2']
            pd['pack_size2'] = info['pack_size2']
            dual.append(pd)
        else:
            normal.append(pd)

    return normal, dual, mixed, unknown

def sort_normal(pages):
    """
    Sort normal pages to match summary order:
    - Products ordered by total label count desc
    - Within each product, pack_size desc
    """
    from collections import defaultdict
    # Count total labels per product
    prod_counts = defaultdict(int)
    for p in pages:
        prod_counts[p.get('primary_product', '')] += 1
    return sorted(pages, key=lambda p: (
        -prod_counts[p.get('primary_product', '')],   # product count desc
        p.get('primary_product', '').lower(),          # then alpha (tiebreak)
        -p.get('pack_size', 0)                         # then pack size desc
    ))

def build_sorted_pdf(all_page_indices, reader, output_path, crop=True):
    """Build PDF from page indices, optionally cropping to top half."""
    writer = PdfWriter()
    for idx in all_page_indices:
        page = reader.pages[idx]
        if crop:
            page = crop_page_top_half(page)
        writer.add_page(page)
    with open(output_path, 'wb') as f:
        writer.write(f)

def build_summary_pdf(account_name, normal_pages, dual_pages, mixed_pages, unknown_pages, master, output_path):
    """Build summary PDF table."""
    # Count by product+packsize for normal pages
    product_counts = defaultdict(int)
    for p in normal_pages:
        key = (p.get('primary_product',''), p.get('pack_size', 0))
        product_counts[key] += 1

    # Mixed counts
    mixed_count = len(mixed_pages)
    unknown_count = len(unknown_pages)

    doc = SimpleDocTemplate(output_path, pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm, topMargin=15*mm, bottomMargin=15*mm)
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph(f"Label Sort Summary — {account_name}", styles['Title']))
    story.append(Spacer(1, 4*mm))

    dual_count = len(dual_pages)
    total = len(normal_pages) + dual_count + mixed_count + unknown_count
    story.append(Paragraph(
        f"Total labels: {total}  |  Single-product: {len(normal_pages)}  |  Dual-product bundles: {dual_count}  |  Mixed: {mixed_count}  |  Unidentified: {unknown_count}",
        styles['Normal']
    ))
    story.append(Spacer(1, 6*mm))

    # Table data
    data = [["#", "Product", "Pack Size", "Labels"]]

    # Sorted product rows
    # Re-sort by count desc within same product
    from collections import OrderedDict
    # Group by product, sort pack desc, then by count
    prod_group = defaultdict(list)
    for (prod, pack), cnt in product_counts.items():
        prod_group[prod].append((pack, cnt))

    # Sort products by total count desc
    prod_totals = {p: sum(c for _, c in items) for p, items in prod_group.items()}
    row_num = 1
    for prod in sorted(prod_group, key=lambda p: -prod_totals[p]):
        for pack, cnt in sorted(prod_group[prod], key=lambda x: -x[0]):
            pack_label = f"Pack {pack}" if pack else "—"
            data.append([row_num, prod, pack_label, cnt])
            row_num += 1

    # Dual-product bundle rows
    dual_counts = defaultdict(int)
    dual_labels = {}  # key -> display string
    for p in dual_pages:
        p1 = p.get('primary_product', '')
        ps1 = p.get('pack_size', 0)
        p2 = p.get('product2', '')
        ps2 = p.get('pack_size2', 0)
        key = (p1, ps1, p2, ps2)
        dual_counts[key] += 1
        dual_labels[key] = f"{p1} (Pack {ps1}) + {p2} (Pack {ps2})"
    for key, cnt in sorted(dual_counts.items(), key=lambda x: -x[1]):
        data.append([row_num, f"📦 {dual_labels[key]}", "Bundle", cnt])
        row_num += 1

    # Unknown rows
    unknown_skus = defaultdict(int)
    for p in unknown_pages:
        sku = p['skus'][0]['sku'] if p['skus'] else 'Unknown'
        unknown_skus[sku] += 1
    for sku, cnt in sorted(unknown_skus.items(), key=lambda x: -x[1]):
        data.append([row_num, f"❓ {sku}", "—", cnt])
        row_num += 1

    table = Table(data, colWidths=[12*mm, 110*mm, 28*mm, 20*mm])
    style = TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#2874f0')),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,0), 10),
        ('ALIGN', (0,0), (-1,-1), 'LEFT'),
        ('ALIGN', (3,0), (3,-1), 'CENTER'),
        ('FONTSIZE', (0,1), (-1,-1), 9),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#f5f5f5')]),
        ('GRID', (0,0), (-1,-1), 0.4, colors.HexColor('#cccccc')),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('LEFTPADDING', (0,0), (-1,-1), 6),
    ])

    # Highlight top products (count >= 5)
    for i, row in enumerate(data[1:], 1):
        if isinstance(row[3], int) and row[3] >= 5 and not str(row[1]).startswith('❓'):
            style.add('BACKGROUND', (0,i), (-1,i), colors.HexColor('#d5e8d4'))
            style.add('FONTNAME', (0,i), (-1,i), 'Helvetica-Bold')
        if str(row[1]).startswith('📦'):
            style.add('BACKGROUND', (0,i), (-1,i), colors.HexColor('#e8f4fd'))
            style.add('FONTNAME', (0,i), (-1,i), 'Helvetica-Bold')
        if str(row[1]).startswith('❓'):
            style.add('BACKGROUND', (0,i), (-1,i), colors.HexColor('#f8d7da'))

    table.setStyle(style)
    story.append(table)

    # ── Mixed Orders Detail Section ──
    if mixed_pages:
        story.append(Spacer(1, 8*mm))
        story.append(Paragraph("⚡ Mixed Orders Detail", styles['Heading2']))
        story.append(Spacer(1, 3*mm))
        story.append(Paragraph(
            "Each row below is one label. Pack the items listed together in one shipment.",
            styles['Normal']
        ))
        story.append(Spacer(1, 4*mm))

        mixed_data = [["#", "Product", "Qty"]]
        for idx, mp in enumerate(mixed_pages, 1):
            skus = mp.get('skus', [])
            # Consolidate duplicate SKUs: sum quantities, show product name
            consolidated = {}  # sku -> {label, qty}
            for s in skus:
                sku_key = s['sku']
                info = master.get(sku_key)
                if info and info['product']:
                    display = f"{info['product']} (Pack {info['pack_size']})"
                else:
                    display = sku_key
                if sku_key in consolidated:
                    consolidated[sku_key]['qty'] += s['qty']
                else:
                    consolidated[sku_key] = {'label': display, 'qty': s['qty']}
            sku_lines = [v['label'] for v in consolidated.values()]
            qty_lines = [str(v['qty']) for v in consolidated.values()]
            mixed_data.append([idx, "\n".join(sku_lines), "\n".join(qty_lines)])

        mixed_table = Table(mixed_data, colWidths=[12*mm, 138*mm, 20*mm])
        mixed_style = TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#e67e22')),
            ('TEXTCOLOR', (0,0), (-1,0), colors.white),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE', (0,0), (-1,0), 10),
            ('ALIGN', (0,0), (-1,-1), 'LEFT'),
            ('ALIGN', (2,0), (2,-1), 'CENTER'),
            ('FONTSIZE', (0,1), (-1,-1), 8),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.HexColor('#fffbf0'), colors.HexColor('#fff3cd')]),
            ('GRID', (0,0), (-1,-1), 0.4, colors.HexColor('#cccccc')),
            ('TOPPADDING', (0,0), (-1,-1), 5),
            ('BOTTOMPADDING', (0,0), (-1,-1), 5),
            ('LEFTPADDING', (0,0), (-1,-1), 6),
            ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ])
        mixed_table.setStyle(mixed_style)
        story.append(mixed_table)

    doc.build(story)

# ─────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/sort', methods=['POST'])
def sort_labels():
    try:
        # Save uploaded files
        sku_file = request.files.get('sku_csv')
        pdf_files = request.files.getlist('pdfs')

        if not sku_file or not pdf_files:
            return jsonify({'error': 'Missing SKU CSV or PDF files'}), 400

        master = load_sku_master(sku_file.read())

        # Save PDFs to temp
        pdf_paths = []
        for f in pdf_files:
            path = os.path.join(UPLOAD_FOLDER, f.filename)
            f.save(path)
            pdf_paths.append(path)

        # ── Step 1: Read all pages, detect account ──
        # account_name -> list of (source_pdf_path, page_idx_in_source, page_data)
        account_pages = defaultdict(list)  # account -> [{orig_path, orig_idx, skus, text}]

        for pdf_path in pdf_paths:
            reader = PdfReader(pdf_path)
            with pdfplumber.open(pdf_path) as plumber_pdf:
                for i, page in enumerate(plumber_pdf.pages):
                    text = page.extract_text() or ''
                    account = detect_account(text)
                    skus = extract_skus_from_page(text)
                    account_pages[account].append({
                        'orig_path': pdf_path,
                        'orig_idx': i,
                        'skus': skus,
                        'text': text,
                    })

        # ── Step 2: Per-account: classify, sort, build PDFs ──
        output_files = []

        for account, pages in account_pages.items():
            # Get this account's SKU lookup from the correct sheet
            account_sku_map = get_account_master(master, account)
            normal, dual, mixed, unknown = classify_pages(pages, account_sku_map)
            sorted_normal = sort_normal(normal)
            sorted_dual   = sort_normal(dual)  # sort dual bundles by primary product count

            # Final page order mirrors summary: normal → dual bundles → unknown → mixed
            ordered = sorted_normal + sorted_dual + unknown + mixed

            # Build one merged reader per source PDF (cache)
            readers = {}
            def get_reader(path):
                if path not in readers:
                    readers[path] = PdfReader(path)
                return readers[path]

            # Build sorted+cropped labels PDF
            safe_name = re.sub(r'[^A-Za-z0-9_]', '_', account)
            labels_path = os.path.join(UPLOAD_FOLDER, f'{safe_name}_sorted_labels.pdf')
            writer = PdfWriter()
            for pd_item in ordered:
                src_reader = get_reader(pd_item['orig_path'])
                page = src_reader.pages[pd_item['orig_idx']]
                writer.add_page(page)
            with open(labels_path, 'wb') as f:
                writer.write(f)

            # Build summary PDF
            summary_path = os.path.join(UPLOAD_FOLDER, f'{safe_name}_summary.pdf')
            build_summary_pdf(account, normal, dual, mixed, unknown, account_sku_map, summary_path)

            output_files.append({
                'name': account,
                'total': len(pages),
                'sku_count': len(normal),
                'labels_file': os.path.basename(labels_path),
                'summary_file': os.path.basename(summary_path),
            })

        return jsonify({'accounts': output_files})

    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/download/<filename>')
def download(filename):
    path = os.path.join(UPLOAD_FOLDER, filename)
    if not os.path.exists(path):
        return "File not found", 404
    return send_file(path, as_attachment=True)

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 5050))
    print(f"🏷️  Flipkart Label Sorter running at http://localhost:{port}")
    app.run(host='0.0.0.0', port=port, debug=False)
