#!/usr/bin/env python3
"""
download_gtex.py
Fetch the GTEx v8 gene-read matrix (GCT) and the sample-metadata file,
then save them under data_raw/.
"""

import pathlib
import urllib.request

# ── directory setup ──────────────────────────────────────────────────────────────
BASE = pathlib.Path(__file__).resolve().parents[1]   # project root
RAW  = BASE / "data_raw"
RAW.mkdir(exist_ok=True)

# ── file table: local-name, URL ──────────────────────────────────────────────────
FILES = {
    "gct": (
        "GTEx_gene_reads.gct.gz",
        "https://storage.googleapis.com/gtex_analysis_v8/rna_seq_data/"
        "GTEx_Analysis_2017-06-05_v8_RNASeQCv1.1.9_gene_reads.gct.gz",
    ),
    "samp": (
        "GTEx_sample_attrs.txt",
        "https://storage.googleapis.com/gtex_analysis_v8/annotations/"
        "GTEx_Analysis_v8_Annotations_SampleAttributesDS.txt",
    ),
}

# ── download helper ──────────────────────────────────────────────────────────────
def fetch(tag: str, fname: str, url: str) -> pathlib.Path:
    """
    Download *url* to data_raw/fname unless it already exists.
    Returns the destination Path.
    """
    dest = RAW / fname
    if dest.exists():
        print(f"✓ {fname} exists, skip")
        return dest
    print(f"⇣ downloading {tag} …")
    urllib.request.urlretrieve(url, dest)
    print("  saved", dest)
    return dest

# ── main ─────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    for tag, (fname, url) in FILES.items():
        fetch(tag, fname, url)