# Berkeley Lab Distributed Solar — mid-scale investment analysis

Working notes for the LBNL **Distributed Solar Public Data** file, release
`TTS_LBNL_public_file_28-Jul-2026_all`.

This README records the **decisions and corrections** made while building the
pipeline. The scripts say *what* the code does; this says *why*, and what we
found out about the data along the way that isn't obvious from reading it.

---

## Running it

```r
# Open berkeley.Rproj in RStudio, then:
source("01_explore_tts_coverage.R")
```

For the analysis, that's the only script you need. If the parquet doesn't exist
yet it builds it first by calling `00_csv_to_parquet.R`, so a fresh clone with
only the CSV works end to end. Results print to the console and write to
`output/` as CSVs.

For the IOF project universe, also run `source("02_build_tts_systems.R")`,
which takes about 15 seconds. It writes `output/tts_systems.parquet`. See
[Export for the project universe](#export-for-the-project-universe).

| File | Role |
| --- | --- |
| `00_csv_to_parquet.R` | CSV → parquet, file paths, `connect_tts()`, and the **shared definitions** (`ZIP5_SQL`, `LIKELY_COMMUNITY_SOLAR_SQL`, `DEDUP_WINDOW_MONTHS`, `dedup_cte()`). Auto-run if needed. |
| `01_explore_tts_coverage.R` | The analysis. ~34 queries. |
| `02_build_tts_systems.R` | The export: one row per PV system, all years, shared column names |
| `output/` | One CSV per query, the row-level mid-scale extract, and `tts_systems.parquet` |

To use a new LBNL release, change **one string** — `DATA_RELEASE` at the top of
`00_csv_to_parquet.R` — and delete or move the old parquet.

---

## Research questions

1. How much capital has been deployed into mid-scale solar since 2018?
2. What project categories have attracted investment?
3. What financing structures have been most prevalent?
4. What types of investors have participated?
5. What regions and market segments have grown most?
6. Where are the investment gaps and underserved markets?

Verdicts on each are in the published
[coverage assessment](https://claude.ai/code/artifact/66d012ae-89ca-4027-92a0-624c4abeb3f5).
The short version: **RQ4 (investor types) cannot be answered from this file** —
there is no capital-provider, lender, or sponsor field anywhere in the 94
columns. `installer_name` identifies developers, not funders.

---

## Analytical choices we made

These are judgment calls, not facts about the data. Each is a variable at the
top of a script so you can change it and re-run.

### Mid-scale = 100 kW to 2 MW

`MIDSCALE_MIN_KW` / `MIDSCALE_MAX_KW` in `01_explore_tts_coverage.R`. Yields
**18,896 systems**, **7,680 MW-DC** and **$10.8B** reported capital since 2018,
across **25 states**.

**Following NREL**, which defines the midscale PV market as "behind-the-meter
systems between 100 kW and 2 MW" — Heeter, Gagnon & Bird (2016), *Expanding
Midscale Solar*, NREL/TP-6A20-65938
([DOI](https://doi.org/10.2172/1326896) ·
[record](https://research-hub.nlr.gov/en/publications/expanding-midscale-solar-examining-the-economic-potential-barrier/) ·
[PDF](https://docs.nlr.gov/docs/fy16osti/65938.pdf)) —
and consistent with industry "mid-market" usage, which puts the floor at the
same 100 kW ([Sunrock](https://www.sunrockdg.com/blog/navigating-growing-solar-landscape-mid-market-vs-community-solar),
[NuWatt](https://nuwattenergy.com/en/commercial-solar/mid-size)).

> **Changed from 100 kW – 5 MW.** An earlier version used a 5 MW ceiling chosen
> by us without a source. Adopting NREL's 2 MW removes 1,211 systems and
> 3,738 MW-DC, and drops mid-scale from 34.3% to **21.0%** of 2018+ capacity.
> Any figure predating this change is on the old band.

**The data does not mark these boundaries.** Checked directly:

- No mass points at 100 kW or 5 MW. The only spike in the 50–6,000 kW range is
  **1,292 kW** (94 systems) — Minnesota's community solar cap.
- Density decays smoothly through 100 kW (3,498 systems in 75–100 kW, 2,676 in
  100–125, 2,076 in 125–150).
- $/W falls monotonically across every step — 3.45, 3.05, 2.85, 2.63, 2.45,
  2.20, 2.06, 1.68, 1.55 — with no inflection at either bound.
- The one hard discontinuity is at **7 MW**, which is LBNL's ground-mount scope
  cap, not a market boundary.

Where the market *does* change character: **50–100 kW** (residential labelling
collapses from 75.7% to 22.5%) and **2 MW** (third-party ownership jumps from
34% to 56%).

**Other definitions, and how ours relates to them:**

| Source | Definition | Relation to ours |
| --- | --- | --- |
| **NREL/NLR, [NREL/TP-6A20-65938](https://doi.org/10.2172/1326896)** | Midscale = behind-the-meter, **100 kW – 2 MW** | **What we use** |
| [Sunrock](https://www.sunrockdg.com/blog/navigating-growing-solar-landscape-mid-market-vs-community-solar) / [NuWatt](https://nuwattenergy.com/en/commercial-solar/mid-size) | "Mid-market" = above 100 kW | Same floor |
| [EIA](https://www.eia.gov/outlooks/steo/report/BTL/2023/09-smallscalesolar/article.php) | Small-scale = "less than one megawatt" | Cuts through our band |
| [LBNL Utility-Scale Solar](https://emp.lbl.gov/utility-scale-solar) | Utility-scale = ground-mounted **>5 MW-AC** | Our whole band is distributed |
| [SEIA / Wood Mackenzie](https://seia.org/research-resources/solar-market-insight-report-q4-2025/) | Segments by **market role**, no size threshold | Neither supports nor conflicts |

⚠️ **NREL is now the National Laboratory of the Rockies (NLR), and the old
domain is dead** — `*.nrel.gov` no longer resolves at all, with no redirect, so
every pre-rename citation is a hard 404. Hosts moved to `research-hub.nlr.gov`
and `docs.nlr.gov` on the same paths. The report number and DOI are unchanged
and are the durable citation; prefer the DOI over either host. Same applies to
Sharing the Sun in `../NLR/`.

Note NREL's definition is **behind-the-meter**, which community solar is not.
Community solar clusters in the 1–2 MW bucket, so it sits at the top of our band
rather than outside it — worth stating if a reviewer presses on the BTM wording.

**The EIA split, because reviewers will ask:**

| | systems | MW | $/W | % priced |
| --- | --- | --- | --- | --- |
| EIA small-scale (<1 MW) | 16,955 (89.7%) | 5,123 (66.7%) | $2.44 | 71.0% |
| EIA "utility-scale" (1–2 MW) | 1,941 (10.3%) | 2,557 (33.3%) | $2.06 | 44.0% |

EIA's 1 MW line reflects **who files Form EIA-860**, not a market distinction —
none of our 1–2 MW systems is a merchant generator. But two-thirds of our
capacity is now on EIA's small-scale side, versus 45% under the old 5 MW band,
so the two frames reconcile much more easily than they used to.

⚠️ **EIA's 1 MW is AC; `PV_system_size_DC` is DC.** A 1 MW AC system is roughly
1.2–1.4 MW DC, so splitting our data at 1,000 kW DC does *not* reproduce EIA's
boundary. `inverter_loading_ratio` (47.4% coverage) can estimate the conversion.
SEIA reports in MWdc, so that comparison needs no conversion.

**Coverage sanity check.** SEIA Q3 2025: commercial 554 + community solar 267 =
821 MWdc, roughly 3.3 GWdc/year nationally. Rough only — SEIA's commercial has
no size bounds and quarterly seasonality is real — but useful as an order-of-
magnitude check on any scaling.

**What the 2 MW ceiling excludes.** The 2–5 MW range holds 1,211 systems and
3,738 MW-DC since 2018 — 74.5% ground-mounted and 44.2% third-party owned,
against 46% and 28% inside the band. It is a distinguishable sub-market, and
LBNL would still call it distributed (utility-scale being >5 MW-AC). Worth
reporting as an adjacent cut if the audience expects a broader mid-scale.

### Size buckets are half-open

A system belongs to a bucket if `size >= lower AND size < upper`. So exactly
2,000 kW is *not* mid-scale; it is the first system in the 2–5 MW bucket. Applied
consistently so buckets tile the number line — nothing double-counted at a
boundary, nothing missed between buckets. (An early version mixed `<=` and `<`
and produced a phantom out-of-range row inside a mid-scale table.)

### Project = one address, within a 12-month window

`DEDUP_WINDOW_MONTHS`, now set in `00_csv_to_parquet.R` (Section 3b). See
"Systems vs projects" below.

---

## What we learned about the data

Everything here was verified against the file or the user guide, not assumed.

### 1. Missing values are `-1`, not blank

The guide's stated convention. A naive `sum()` or `avg()` treats them as real
numbers and silently corrupts the result. Every query guards with `> 0` or
`NULLIF(col, -1)`.

Confirmed: `total_installed_price` and `PV_system_size_DC` both have a minimum
of −1 and **zero** true NULLs.

### 2. The dataset's name is not "Tracking the Sun"

The guide is titled *User Guide for the Distributed Solar Public Data*. Tracking
the Sun is the **program** and download site. The acronym survives in the
filenames (`TTS_LBNL_public_file_*`) and one column (`TTS_link_ID`) regardless.

### 3. Scope is not a flat megawatt cap

Rooftop systems of **any size**, plus ground-mount **up to 7 MW-DC**.
Ground-mount above 7 MW is utility-scale and excluded. Verified in the data:
zero ground-mounted systems above 7 MW, 16 rooftop records above it.

### 4. The guide documents 81 fields; the file ships 94

Thirteen columns are undocumented. Trust the parquet schema over the PDF when a
column name is in doubt.

### 5. `customer_segment` describes the customer, not the building

The location fields beside it (`zip_code`, `city`, `state`) are explicitly the
*host customer's*. So `RES_SF` on an 18 MW array is **correct** — under group net
metering a large array's output is credited to residential accounts.

Consequence: **community solar is mislabelled as residential.** Minnesota alone
is **50%** of all `RES` mid-scale records and ~33% of the national 1–2 MW bucket, at
a median of 1,333 kW, with 10 of 664 reporting a price and **none** reporting
ownership. They add megawatts to totals while contributing nothing to dollars.

Query `06e` flags every affected state. The row-level extract carries a
`likely_community_solar` column (890 systems) so the include/exclude call stays
explicit.

The guide lists ten segment types including `NON-PROFIT`; that value appears
**nowhere** in this release. Nine are in use.

### 6. `ground_mounted` is unreliable at the top of the range

The largest record coded rooftop is **140.6 MW**, and eight of the twelve largest
systems are in Vermont. No real rooftop array is 140 MW, and the guide says mixed
rooftop/ground systems are coded *ground*-mounted, so that isn't the explanation
either.

These 16 records carry 355 MW — 8% of the 5 MW+ bucket. **They sit above 7 MW so
no mid-scale figure is affected**, but filter on `PV_system_size_DC` before
computing means or $/W across all size classes.

### 7. Income fields are binned, in thousands

All six `med_inc_*` fields are in thousands of 2025 dollars, **binned to $10k**,
reporting the bin's **lower bound**, capped at 200 = "$200k+". So `60` means
*$60k–$70k*, not $60,000.

Never print them as precise dollars. Query 24's quartile cut points are coarse.
Two tracts showing the same value may differ by up to $10k.

There are two series and they differ:

| | all households | owner-occupied | gap |
| --- | --- | --- | --- |
| All records | $111.7k | $128.1k | +$16.4k |
| Mid-scale tracts | $82.9k | $103.0k | +$20.1k |

Owner-occupied excludes renters. For **residential** adoption equity it is the
fairer benchmark, since rooftop solar effectively requires owning the property.
For **mid-scale** the host is a business or institution, so neither describes the
customer — they describe the surrounding tract. Query 24 uses the all-household
series for that reason.

### 8. Coverage is driven by *which agency* reported

Three source types — incentive programs, interconnection queues, SREC registries
— with different biases. This is why price coverage varies so wildly by state:
CA and NY near 100%, while **NJ, IL, VT, ME, MD, OH and DC report none at all**
for mid-scale. It is not random missingness.

Only **27 states** appear in the file, and mid-scale systems occur in **25** of them. Any total is a 25-state figure.

### 9. `zip_code` is the installation site — and it is malformed

**It is where the system is, not where customers are.** The guide calls it "host
customer zip code", which is ambiguous, so we tested it two ways:

- `TTS_link_ID` is defined as "systems installed at the same address", and
  **98.2%** of those groups share a single zip (208,214 of 212,008). If zip
  tracked customers, systems at one address would scatter across subscriber zips.
- Minnesota's community solar resolves 587 gardens to **119 distinct zips** —
  about five per zip, consistent with array sites spread across the state, not
  with thousands of subscriber households. (`city` is redacted on all 587.)

So for community solar, `customer_segment` describes the **subscribers** while
`zip_code` describes the **array's location** — two different parties in one row.
That is why `med_inc_HH_tract` measures the host's neighbourhood, not the
beneficiaries'. See item 5.

**Only ~32% of zips are directly joinable.** File-wide:

| Format | records | share |
| --- | --- | --- |
| Clean 5-digit | 1,219,495 | 30.5% |
| **Float artifact** `"65014.0"` | **2,218,897** | **55.5%** — all of California |
| **4-digit, leading zero lost** | **247,690** | 6.2% — MA 141,898 · CT 50,028 · VT 27,347 · ME 18,165 · NH 10,244 |
| zip+4 | 73,726 | 1.8% |
| Missing (`-1`) | 234,912 | 5.9% |
| Garbage — phone numbers, `"Clinton"`, tabs, `"0.0"` | 945 | 0.02% |

Two notes. The `.0` suffix is a float-conversion bug upstream — the field was
held as a number somewhere and stringified back. And the 4-digit zips are the
leading-zero destruction we guard against in `00_csv_to_parquet.R`, except it
**already happened before the data reached us**: forcing VARCHAR protects zeros
that still exist, it cannot restore ones LBNL's own pipeline lost. Vermont has
lost the zero on 27,347 of its 27,391 zips.

**Fixed via `ZIP5_SQL`** (defined in Section 1 of the analysis script), which
strips the `.0`, splits off the +4, and zero-pads to five. This recovers
**3,760,730 of 3,760,753** non-missing zips — 23 stragglers are the garbage
values, none of them mid-scale. The row-level extract carries both `zip_code`
(raw) and `zip5` (normalised).

⚠️ **Join on `zip5`, never on `zip_code`.** A raw join silently drops California
and most of New England. Queries `06h` and `06i` report the format mix and
confirm the recovery — re-run them against any new release, since the mix
depends on who reported.

---

## Systems vs projects

One physical installation can appear as several rows: phases of a build, later
expansions, or (file-wide) a PV array and a battery installed separately at one
address.

**Naming convention used throughout the outputs:**

- **`systems`** = rows in the file. What nearly every query counts.
- **`projects`** = distinct addresses after collapsing, within the time window.

### The time window

`TTS_link_ID` groups by **address**, and address is broader than project. Among
the mid-scale groups holding more than one system (509 under the old 5 MW band; the shape is unchanged):

| Installed... | groups |
| --- | --- |
| Within a week | 177 |
| Within 3 months | 78 |
| Within a year | 71 |
| 1–3 years apart | 98 |
| More than 3 years apart | 85 |

A school adding a second array three years later is one address but **two
financing events**. Collapsing them hides a repeat customer — which for a
capital-deployment question is exactly what you want to see.

`DEDUP_WINDOW_MONTHS` (default **12**) sets how far apart systems can be and
still count as one project. Query `06g` reports the sensitivity:

| Window | Systems | Projects | Collapsed |
| --- | --- | --- | --- |
| 0 (none) | 18,896 | 18,896 | 0.00% |
| 1 month | 18,896 | 18,659 | 1.25% |
| 12 months | 18,896 | 18,511 | 2.04% |
| 36 months | 18,896 | 18,412 | 2.56% |
| 999 (any gap) | 18,896 | 18,332 | 2.98% |

The whole range spans 3%, so the choice is not load-bearing — but it is now
explicit rather than accidental.

### When to de-duplicate — and when not to

| Measure | Collapse? | Why |
| --- | --- | --- |
| Project counts | **Yes** | Rows overstate distinct sites by ~3% |
| Capital ($) | **No** | Each phase row carries its **own** cost |
| Capacity (MW) | **No** | Same — each phase added real capacity |
| Shares and percentages | Doesn't matter | Moves by <0.5pp |

**Summing dollars across phase rows is correct.** Verified: of two-row groups,
390 of 430 have different sizes and 225 of 287 different prices; phased rows run
**$2.04/W** against **$2.17/W** for single-row systems — consistent with
incremental costs, not repeated totals. If rows repeated the project total you
would see roughly half that. Collapsing would delete real capital.

Percentages barely move because companion rows agree with each other: 497 of 509
groups share one segment, 474 of 509 one ownership value. Third-party-owned share
goes 28.5% → 28.4%.

**The genuine duplicates** are a much smaller set: 24 groups sharing an identical
link ID, size *and* price to the cent — 28 redundant rows carrying $25.6M, or
0.18% of reported mid-scale capital.

Coverage caveat: only **10.5%** of mid-scale rows carry a link ID at all, so any
de-duplication is a **floor**, not a complete fix.

---

## Pipeline decisions

### Parquet, not CSV

2,311 MB → 127 MB, an 18× reduction, and queries run in seconds instead of
minutes. DuckDB streams the conversion so peak memory stays low regardless of
file size. **Do not** use `read.csv()` then `arrow::write_parquet()` — that loads
4 million rows into RAM first and will thrash. The same trap exists in Python
with `pandas.read_csv()`.

### Identifier columns are forced to text

DuckDB infers types from a sample, and a wrong guess here is silent and
destructive:

- `zip_code` — "02138" would become the integer 2138. There are **230,161**
  leading-zero zips in this file, concentrated in exactly the Northeast states
  where mid-scale activity is. They would silently stop joining to census data.
- `extensions_multiphase_id` — **did** sniff as BIGINT on this file, diverging
  from the schema LBNL ships.

`00_csv_to_parquet.R` pins all five ID columns to VARCHAR and then *asserts* it
worked, stopping the run if any landed as a number.

### Conversion is lossless

Step 0 changes **format**, step 1 changes **meaning**. The `-1` codes are left
intact in the parquet so any number can be traced back to source; they are
handled in the analysis layer via `NULLIF`. Mixing the two makes it impossible to
tell whether a surprising result came from the data or from your own cleaning.

Verified identical to LBNL's parquet: row counts, capacity sums, price sums,
distinct states, distinct zips, and the mid-scale count all match. The only
schema difference is `installation_date` typed `TIMESTAMP` vs `TIMESTAMP_NS` — a
storage-precision label, same values, same query behaviour.

### `here()` for paths

No absolute paths anywhere, so the project runs on any machine. The release is
named explicitly rather than auto-detected — the filename is part of the
provenance, and auto-detection by modification time would let an OneDrive
re-sync silently change which vintage you analysed.

---

## Export for the project universe

`02_build_tts_systems.R` writes `output/tts_systems.parquet`: **3,937,830 PV
systems × 53 columns**, all years, about 110 MB. It is Tracking the Sun's
contribution to the IOF one-row-per-project universe. The shared columns use
the same names as the EIA, USPVDB, FERC 556 and Sharing the Sun outputs in the
sibling folders. Linking to those happens later, in the merge script.

**Decisions**

- **Rows.** Every row with a PV size, all years. `in_window` marks the
  2,611,474 installed in 2018 or later. Storage-only rows (52,922) are dropped.
  So are 4,913 other rows with no PV size. 2,402 storage-only rows from 2018
  on also carry a PV size, because they are batteries added to an existing
  array, and they go with the storage-only rows. Rows stay systems and are not
  collapsed, for the capital reasons above.
- **Record ID.** `source_record_id` = `tts_` plus the row's position in the
  release file. LBNL's provider + system ID pair is not unique and is missing
  on about 93,000 rows, so it is kept but not used as the key. The position is
  stable within a release, not across releases.
- **AC capacity.** Tracking the Sun reports DC only. `capacity_mw_ac` = DC ÷
  loading ratio, with `capacity_ac_method` saying which ratio was used:

  | Ratio used | Systems |
  | --- | ---: |
  | The system's own `inverter_loading_ratio`, when between 0.8 and 2.0 | 1,657,711 |
  | Median plausible ratio for the same install year × customer segment, then year | 2,280,119 |

  126,000 reported ratios fall below 0.8, some near 0.5, and look like errors.
  The raw value stays in `inverter_loading_ratio_raw`. Tracking the Sun's own
  medians are used, 1.12 to 1.26 for 2018–2025, rather than EIA's
  utility-scale 1.33.
- **Cleaning.** -1 becomes NULL in every cleaned column; the parquet from step
  0 still holds the raw codes. Flags coded 1 / 0 / -1 become TRUE / FALSE /
  NULL.
- **Grouping.** `is_first_at_address` and `first_install_at_address` use the
  shared 12-month rule. `exact_duplicate` marks rows with the same link ID,
  size and price to the cent; it applies only when a price is reported.

**In-window flags:** 986 `likely_community_solar`, 3,019 `exact_duplicate`,
19,833 later phases at an address already counted. `zip5` is blank for 1,463
rows whose `zip_code` is "redacted". 22 malformed ZIPs can't be normalised.

**Shared definitions moved to `00`.** `ZIP5_SQL`, `LIKELY_COMMUNITY_SOLAR_SQL`,
`DEDUP_WINDOW_MONTHS` and `dedup_cte()` now live in `00_csv_to_parquet.R`,
Section 3b, so 01 and 02 can't drift apart. Change the address window there.
`dedup_cte()` gained a tie-breaker, `data_provider_1` and `system_ID_1`, so the
same row is marked first on every run; counts are unchanged. After the move, 29
of 01's 34 outputs are byte-identical to before. The other five differ only
where 01 already varied from run to run. Which files differ changes from run
to run:
- rows tied in an `ORDER BY`, seen in 06e, 10, 17 and 23
- the `ntile(4)` income quartiles in 24, which split tied incomes arbitrarily
- which of two tied systems is marked first in the mid-scale extract, now
  fixed by the tie-breaker

---

## Known gaps / next steps

- **RQ4 (investors)** needs an external join. Developer names from query 16 are
  the join key into ownership and capital-stack data.
- **PPA vs lease** is not distinguishable — `third_party_owned` is one binary.
  No tax equity, C-PACE, or project debt anywhere in the file.
- **Energy produced** is not in the file at all; every figure is nameplate
  capacity. For an energy framing, join to PVWatts (the file has tilt, azimuth
  and location) or EIA-923.
- **The direct-pay question is open.** School TPO share falls 52.2% (2018–2022)
  → 40.3% (2023–2025), consistent with IRA elective pay removing the reason for
  third-party ownership. But commercial — which is taxable and unaffected —
  also falls, 24.5% → 15.6%, so credit transferability may be a common cause.
  A within-California test would remove the state-mix confound; CA has 1,229
  school systems, enough to run it.
