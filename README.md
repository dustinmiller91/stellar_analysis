# Stellar Physical Characteristics Analysis

Exploratory analysis of stellar physical properties across spectral and luminosity classes,
using two publicly available catalogs from the Strasbourg Astronomical Data Center (CDS).

## Why these datasets?

| Catalog | Source | Size | Why |
|---------|--------|------|-----|
| [Hipparcos](https://cds.unistra.fr/cgi-bin/Cat?I/239) | ESA (1997) | ~25 MB | 118k stars across the whole sky; well-studied, precise parallaxes |
| [Woolley](https://cds.unistra.fr/cgi-bin/Cat?V/32A) | Woolley et al. (1970) | ~1 MB | 2,150 stars within 25 pc; corrects for Hipparcos luminosity bias |

Using both is deliberate: Hipparcos is magnitude-limited and over-represents bright giants;
Woolley is distance-limited and captures the faint main-sequence dwarfs that dominate the
actual local stellar population.

## Prerequisites

- Python 3.12+
- `sqlite3` (stdlib)
- `stellar_parser.py` (included — parses MK spectral type strings)

Install dependencies:

```bash
pip install -r requirements.txt
```

## Repository layout

```
.
├── etl.py                          # Download → bronze → silver → gold pipeline
├── stellar_parser.py               # Spectral type string parser
├── stellar_analysis.ipynb  # Analysis notebook (run after ETL)
├── schema.sql                      # DDL for all tables (documentation / reference)
├── requirements.txt
└── README.md
```

## How to run

### 1. ETL (builds the database)

```bash
python etl.py                        # writes to stellar_analysis.db (default)
python etl.py --db /path/to/out.db   # custom path
```

Running twice is safe — all stages are idempotent (bronze uses DROP/CREATE,
silver uses `to_sql(if_exists="replace")`, gold drops and recreates).

### 2. Analysis notebook

```bash
jupyter notebook stellar_analysis.ipynb
```

Or run headless:

```bash
jupyter nbconvert --to notebook --execute stellar_analysis.ipynb
```

The notebook assumes `stellar_analysis.db` is in the working directory.
Override by editing the `DB` variable at the top of the Setup cell.

## Schema overview

```
hipparcos_bronze  ──┐                    (raw TEXT columns, one-to-one with CSV)
woolley_bronze    ──┤
                    ▼
hipparcos_silver  ──┐  (typed + parsed: REAL/INTEGER casts, sp_class/lum_class added)
woolley_silver    ──┤
                    ▼
stars                  (gold: union of both silver tables, quality-filtered,
                         dist_pc and abs_mag derived, indexed for analysis)
```

See `schema.sql` for full DDL.

## Analysis questions answered

| # | Type | Question |
|---|------|----------|
| 1 | SQL | Does Hipparcos exhibit luminosity bias vs. Woolley? |
| 2 | SQL | How does the luminosity-class mix change with distance? |
| 3 | SQL | Median colour and absolute magnitude by spectral × luminosity class |
| 4 | Python | Absolute magnitude distributions per luminosity class (histograms) |
| 5 | Python | Hertzsprung-Russell diagrams — full dataset and per luminosity class |

## Known limitations / data quality notes

- **Woolley parallaxes** are in integer milliarcseconds (lower precision than Hipparcos).
- No interstellar reddening correction is applied to B-V colour.
- Parallax error filtering is not applied in the gold table (retained for completeness);
  high-error entries at large distances may inflate scatter.
- Woolley luminosity class is stored as integer `lc_code`; mapping to roman numerals
  is done in `etl.py::fix_woolley_lum_class`.
