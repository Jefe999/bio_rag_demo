#!/usr/bin/env python3
"""
etl_gtex.py
• Loads GTEx v8 gene-read matrix and sample-metadata from data_raw/
• Keeps only liver samples
• Saves a compact Parquet table in data_parquet/
• Logs run metadata + artifact in MLflow
"""

import gzip, io, pathlib
import pandas as pd
import pyarrow as pa, pyarrow.parquet as pq
import mlflow

# ── paths ───────────────────────────────────────────────────────────────────────
BASE = pathlib.Path(__file__).resolve().parents[1]
RAW  = BASE / "data_raw"
PQ   = BASE / "data_parquet"
PQ.mkdir(exist_ok=True)

gct_path   = RAW / "GTEx_Analysis_2017-06-05_v8_RNASeQCv1.1.9_gene_reads.gct.gz"   # or .gct if you renamed
meta_path  = RAW / "GTEx_Analysis_v8_Annotations_SampleAttributesDS.txt"

# ── smart file-open helper ──────────────────────────────────────────────────────
def smart_open(path: pathlib.Path):
    """
    Return a text handle that works whether *path* is true gzip or plain file even
    if the extension is .gz.
    """
    with open(path, "rb") as hdr:
        magic = hdr.read(2)
    if magic == b"\x1f\x8b":          # gzip magic bytes
        return gzip.open(path, "rt")
    return open(path, "rt")

# ── load metadata + pick liver samples ──────────────────────────────────────────
meta = pd.read_csv(meta_path, sep="\t", low_memory=False)
liver_ids = meta.loc[meta.SMTSD == "Liver", "SAMPID"]
print("liver samples:", len(liver_ids))

# ── read GCT lazily # ── discover actual sample columns ─────────────────────────────────────────────
with smart_open(gct_path) as fhdr:
    next(fhdr); next(fhdr)                       # skip first 2 header lines
    col_names = fhdr.readline().rstrip("\n").split("\t")

gct_samples = set(col_names[2:])                 # drop Name & Description
keep_ids = [sid for sid in liver_ids if sid in gct_samples]
print(f"liver IDs requested: {len(liver_ids)}  — found in GCT: {len(keep_ids)}")

# ── load the matrix (only columns present) ─────────────────────────────────────
df = pd.read_csv(
    smart_open(gct_path),
    sep="\t",
    skiprows=2,
    usecols=["Name", "Description", *keep_ids],
)
# ── tidy & save Parquet ─────────────────────────────────────────────────────────
df.rename(columns={"Name": "gene_id", "Description": "gene_symbol"}, inplace=True)
parquet_out = PQ / "gtex_liver.parquet"
pq.write_table(pa.Table.from_pandas(df), parquet_out, compression="zstd")
print("✔ Parquet saved →", parquet_out)

# ── MLflow lineage ──────────────────────────────────────────────────────────────
mlflow.set_tracking_uri(f"file://{BASE}/mlruns")
mlflow.set_experiment("gtex_demo")        # auto-creates if missing
with mlflow.start_run(run_name="gtex_etl"):
    mlflow.log_param("gct_file", gct_path.name)
    mlflow.log_param("meta_file", meta_path.name)
    mlflow.log_metric("liver_samples", len(liver_ids))
    mlflow.log_metric("genes", df.shape[0])
    mlflow.log_artifact(str(parquet_out))