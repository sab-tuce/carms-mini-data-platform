import pandas as pd
import re

m = pd.read_excel("data/raw/1503_program_master.xlsx")
x = pd.read_csv("data/raw/1503_program_descriptions_x_section.csv", low_memory=False)

print("master columns:", list(m.columns))
print("x_section columns:", list(x.columns))

print("\n--- samples ---")
print("\nmaster sample:")
print(m[["program_stream_id","program_url"]].head(5).to_string(index=False))

print("\nx_section sample:")
cols = [c for c in ["document_id","source","program_name","match_iteration_name"] if c in x.columns]
print(x[cols].head(5).to_string(index=False))

# 1) Try join by URL: program_url == source
if "program_url" in m.columns and "source" in x.columns:
    merged_url = m.merge(x, left_on="program_url", right_on="source", how="inner")
    print("\n--- join test: program_url == source ---")
    print("matched rows:", len(merged_url), "out of", len(m))

# 2) Try extract numeric id from document_id and match to program_stream_id
print("\n--- join test: extract id from document_id ---")
if "document_id" in x.columns and "program_stream_id" in m.columns:
    doc = x["document_id"].astype(str)

    # extract last number in the string (works for '1503-27672' or '...27672')
    extracted = doc.str.extract(r"(\\d+)$")[0]

    ok = extracted.notna().sum()
    print("document_id non-null:", doc.notna().sum(), " | extracted numeric ids:", ok)

    x2 = x.copy()
    x2["extracted_program_stream_id"] = pd.to_numeric(extracted, errors="coerce")

    merged_id = m.merge(x2, left_on="program_stream_id", right_on="extracted_program_stream_id", how="inner")
    print("matched rows via extracted id:", len(merged_id), "out of", len(m))

    # show a few mismatches if any
    m_ids = set(m["program_stream_id"].dropna().astype(int).tolist())
    x_ids = set(x2["extracted_program_stream_id"].dropna().astype(int).tolist())
    print("missing in x (by extracted):", len(m_ids - x_ids))
    print("extra in x (by extracted):", len(x_ids - m_ids))

else:
    print("document_id or program_stream_id not found.")

