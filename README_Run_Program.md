# Merit Aid Analysis — How to Run

Two scripts. Run in order.

## 1. Setup

```bash
pip install pyodbc pandas numpy plotly
```

**Windows only.** `CSV_Conversion.py` needs the free **Microsoft Access Database Engine** (ODBC driver) installed. Get it from Microsoft's download page, match 32/64-bit to your Python install. This step won't work on Mac/Linux — Access drivers are Windows-only.

## 2. Get the data

Download IPEDS 2022-23 Access database (`.accdb`) from nces.ed.gov/ipeds/use-the-data.

## 3. Run Script 1 — `CSV_Conversion.py`

Turns the Access database into CSV files.

Open the file, edit these two lines at the top:
```python
ACCD_FILE = r"C:\path\to\IPEDS202223.accdb"   # your downloaded file
OUTPUT_DIR = r"C:\path\to\output_folder"      # where CSVs will go
```

Run:
```bash
python CSV_Conversion.py
```

This exports **every** table in the database as its own CSV. You only need 8 of them for step 4 (see below).

## 4. Run Script 2 — `IPEDS_Merit_Aid_Analysis.py`

Ranks colleges and builds the charts.

Edit this line at the top:
```python
IPEDS_BASE_PATH = r"C:\path\to\output_folder"  # same folder as OUTPUT_DIR above
```

Make sure these 8 files are in that folder (exact names required):
| Needed file | Contains |
|---|---|
| HD2022.csv | College directory |
| SFA2122_P1.csv | Financial aid, part 1 |
| SFA2122_P2.csv | Financial aid, part 2 |
| IC2022_AY.csv | Sticker price / charges |
| ADM2022.csv | SAT scores |
| DRVADM2022.csv | Admissions rate |
| DRVGR2022.csv | Graduation rate |
| IC2022Mission.csv | Mission statements |

Run:
```bash
python IPEDS_Merit_Aid_Analysis.py
```

**What happens:**
1. Loads and cleans the 8 files
2. Filters to 4-year colleges only
3. Calculates MGI = Institutional Grant ÷ Sticker Price
4. Filters to colleges with net price ≤ $25,000 and above-median MGI
5. Ranks by Composite Score, keeps top 20
6. Saves `final_merit_college_rankings.csv` in the same folder
7. Opens two charts in your browser (Dumbbell chart, Parallel Coordinates chart)

## Known gaps (be ready to mention these if asked)

- **No RAG/AI tool built yet.** Slide 7 describes it, but the code doesn't include it — mission statement text is loaded but never used for search/Q&A.
- **Paths are hardcoded**, not passed as arguments — anyone running this must manually edit the file paths.
- **No `main.py` or single entry point** — two scripts run separately, by hand.
- **Windows/Access dependency** — the pipeline can't run on Mac/Linux as-is because of the Access driver requirement.
