import pandas as pd

m = pd.read_excel("data/raw/1503_program_master.xlsx")
x = pd.read_csv("data/raw/1503_program_descriptions_x_section.csv", low_memory=False)

# drop old excel index cols if exist
if "Unnamed: 0" in m.columns:
    m = m.drop(columns=["Unnamed: 0"])
if "Unnamed: 0" in x.columns:
    x = x.drop(columns=["Unnamed: 0"])

# document_id like: "1503-27447"
doc = x["document_id"].astype(str).str.strip()

# split into two parts from the right: [left, right]
parts = doc.str.rsplit("-", n=1, expand=True)

# second part (id)
right = parts[1] if parts.shape[1] > 1 else pd.Series([None] * len(doc))

extracted = pd.to_numeric(right.str.strip(), errors="coerce")

master_ids = pd.to_numeric(m["program_stream_id"], errors="coerce")

master_set = set(master_ids.dropna().astype(int).tolist())
extracted_set = set(extracted.dropna().astype(int).tolist())

print("rows master:", len(m))
print("rows x_section:", len(x))
print("\nextracted id count (non-null):", int(extracted.notna().sum()))
print("extracted unique ids:", len(extracted_set))

missing = master_set - extracted_set
extra = extracted_set - master_set

print("\nmissing vs master:", len(missing))
print("extra vs master:", len(extra))

# show a few examples if any missing/extra
if len(missing) > 0:
    print("\nFirst 10 missing ids (in master not in extracted):")
    print(list(sorted(missing))[:10])

if len(extra) > 0:
    print("\nFirst 10 extra ids (in extracted not in master):")
    print(list(sorted(extra))[:10])

# if anything failed to extract, show sample repr (to catch hidden chars)
if extracted.notna().sum() < len(x):
    print("\nSome document_id values did not extract. Sample repr:")
    for s in doc[extracted.isna()].head(10).tolist():
        print(repr(s))

# show a few mismatched rows (where extracted exists but not in master)
bad_rows = x.loc[extracted.notna() & ~extracted.astype(int).isin(master_set), ["document_id", "source"]].head(10)
if len(bad_rows):
    print("\nExamples where extracted id NOT in master:")
    print(bad_rows.to_string(index=False))

# final verdict
ok = (len(missing) == 0 and len(extra) == 0 and extracted.notna().sum() == len(x))
print("\n✅ FINAL:", "PASS (815/815 match)" if ok else "FAIL (see counts above)")
