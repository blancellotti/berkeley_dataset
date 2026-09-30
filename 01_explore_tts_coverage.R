# =============================================================================
# Tracking the Sun (LBNL) -- Mid-Scale Solar Investment Coverage Assessment
# =============================================================================
#
# PURPOSE
#   Assess what the LBNL "Tracking the Sun" (TTS) public data file can and
#   cannot tell us about capital deployed into MID-SCALE solar since 2018.
#
#   This is a COVERAGE script. Before answering research questions we need to
#   know: how many records exist, how many have the fields we need, and where
#   the holes are. Every query below reports counts alongside sums so you can
#   always see how much of the data is actually behind a number.
#
# -----------------------------------------------------------------------------
# NAMING CONVENTION IN THE OUTPUT: "SYSTEMS" vs "PROJECTS"
# -----------------------------------------------------------------------------
#   systems   = ROWS in the file. One physical installation built in phases
#               appears as several systems. This is what almost every query
#               below counts, and every column named `systems`, `n_systems`
#               or `midscale_systems` is on this basis.
#
#   projects  = DISTINCT ADDRESSES, after collapsing phases and expansions via
#               TTS_link_ID. Only query 06f reports this, and it shows both
#               bases side by side so you can see the difference.
#
#   For mid-scale since 2018 see query 06f for the systems-vs-projects gap --
#   about 3%. Shares and percentages barely move (see Section 1), so the
#   distinction matters when you state a COUNT, not when you state a share.
#
#   Money and capacity are always reported on the systems basis and should
#   stay that way -- each phase row carries its own cost. Section 1 explains.
#
# HOW TO RUN
#   Open berkeley.Rproj in RStudio, then open this file and press
#   Ctrl+Shift+Enter (Source). Or run section by section with Ctrl+Enter.
#   Results print to the console AND get written to ./output as CSVs.
#
#   This is the ONLY script you need to run. If the parquet file doesn't exist
#   yet, this script builds it automatically by calling 00_csv_to_parquet.R --
#   so on a fresh machine with only the LBNL CSV, sourcing this file runs the
#   whole pipeline start to finish.
#
# PIPELINE
#   00_csv_to_parquet.R        data prep: CSV -> parquet, plus the file paths
#                              and connect_tts(). Auto-run if needed.
#   01_explore_tts_coverage.R  <- you are here (the analysis)
#
# -----------------------------------------------------------------------------
# BACKGROUND: WHAT IS A PARQUET FILE, AND WHY DUCKDB?
# -----------------------------------------------------------------------------
#   A CSV stores data as text, row by row. To answer "what's the average price?"
#   your computer must read every character of every row -- all 2.3 GB of the
#   CSV version of this file.
#
#   A PARQUET file stores the same data COLUMN by column, compressed. The same
#   dataset is 153 MB instead of 2,300 MB. If your query only touches 3 of the
#   94 columns, only those 3 columns get read off disk. That is why the queries
#   below finish in seconds.
#
#   DUCKDB is a database engine that runs inside your R session -- no server,
#   no install wizard, no separate program. You hand it SQL, it reads the
#   parquet directly off disk, and hands back a normal R data.frame.
#
#   The key advantage over read.csv(): the data NEVER fully loads into R's
#   memory. DuckDB does the filtering and summing on disk and returns only the
#   small summary table. You can query a 4-million-row file on a laptop.
#
# -----------------------------------------------------------------------------
# BACKGROUND: SQL IN FIVE MINUTES
# -----------------------------------------------------------------------------
#   Every query in this script follows the same shape. Read it top to bottom:
#
#     SELECT    <-- which columns do I want back? (the output)
#     FROM      <-- which table am I reading? (here: always "tts")
#     WHERE     <-- which ROWS do I keep? (a filter, applied before grouping)
#     GROUP BY  <-- collapse rows into buckets (e.g. one row per state)
#     HAVING    <-- filter the BUCKETS (applied after grouping)
#     ORDER BY  <-- how do I sort the output?
#     LIMIT     <-- only give me the top N rows
#
#   In dplyr terms, if that helps:
#     SELECT   ~ select() / mutate() / summarise()
#     WHERE    ~ filter()   (before group_by)
#     GROUP BY ~ group_by()
#     HAVING   ~ filter()   (after group_by)
#     ORDER BY ~ arrange()
#     LIMIT    ~ head()
#
#   AGGREGATE FUNCTIONS collapse many rows into one number:
#     count(*)      how many rows
#     sum(x)        total of column x
#     avg(x)        mean of x
#     median(x)     median of x
#     min(x)/max(x) smallest / largest
#
#   TWO IDIOMS USED HEAVILY BELOW, worth learning now:
#
#   1. count(*) FILTER (WHERE <condition>)
#      Counts only the rows matching the condition, WITHIN each group. This
#      lets you report "3,000 projects, of which 2,100 report a price" in a
#      single query. It is the workhorse of coverage assessment.
#
#   2. NULLIF(x, -1)
#      Returns NULL (SQL's "missing") when x equals -1, otherwise returns x.
#      Critical here -- see the missing-data note below.
#
# -----------------------------------------------------------------------------
# !! THE MOST IMPORTANT THING TO KNOW ABOUT THIS DATASET !!
# -----------------------------------------------------------------------------
#   TTS codes MISSING VALUES AS -1, not as blank/NA. If you naively run
#   sum(total_installed_price) or avg(PV_system_size_DC), those -1s are treated
#   as real numbers and silently corrupt your answer.
#
#   Confirmed in this file: total_installed_price has a minimum of -1, and
#   PV_system_size_DC has a minimum of -1. Neither has any true NULLs.
#
#   Every query below therefore either filters with `> 0` or wraps the column
#   in NULLIF(col, -1). SQL aggregates ignore NULLs automatically, so once a
#   -1 becomes NULL it stops polluting sums and averages.
#
#   Also watch for the literal string "redacted" in text columns -- some data
#   providers require LBNL to suppress project details.
#
# =============================================================================


# =============================================================================
# SECTION 0 -- SETUP
# =============================================================================

# Install once if needed, then comment out:
# install.packages(c("DBI", "duckdb"))

# --- Load the data-preparation script ---------------------------------------
# Script 00 loads the libraries, names the data release, resolves the file
# paths via here(), and defines connect_tts(). Sourcing it here means no paths
# are hardcoded in this file, and the two scripts can never disagree about
# which data they're reading.
#
# PIPELINE_SOURCED tells 00 "I only want your paths and helpers -- do NOT start
# converting". Without it, sourcing 00 would kick off a 70-second rebuild every
# time you ran this script. The conversion still happens automatically if the
# parquet is genuinely missing; connect_tts() handles that below.
#
# This one line assumes your working directory is the project folder -- true if
# you opened berkeley.Rproj, which is the documented way to run this.
# Everything after it is location-independent, because here() takes over.

PIPELINE_SOURCED <- TRUE
source("00_csv_to_parquet.R")

output_dir <- OUTPUT_DIR

# --- Connect ----------------------------------------------------------------
# connect_tts() does three things:
#   1. checks that the parquet exists, and RUNS 00_csv_to_parquet.R to build it
#      if it doesn't -- so this script works even on a fresh clone with only
#      the CSV present;
#   2. opens an in-memory DuckDB engine (nothing is written to disk, it is just
#      a query engine living in your R session);
#   3. registers the parquet as a table named `tts`, so every query below can
#      just say FROM tts.
#
# The first run on a machine without a parquet will therefore pause for ~1
# minute to do the conversion. Every run after that is instant.

con <- connect_tts()

# --- Two small helpers ------------------------------------------------------
# q()  runs a SQL string and returns a data.frame.
# run() runs it, prints it with a title, saves it to output/<name>.csv, and
#       invisibly returns it so you can assign it if you want to plot it later.

q <- function(sql) dbGetQuery(con, sql)

run <- function(name, title, sql) {
  cat("\n\n========================================================\n")
  cat(title, "\n")
  cat("========================================================\n")
  res <- q(sql)
  print(res, row.names = FALSE, right = FALSE)
  write.csv(res, file.path(output_dir, paste0(name, ".csv")), row.names = FALSE)
  invisible(res)
}


# =============================================================================
# SECTION 1 -- ANALYTICAL DEFINITIONS
# =============================================================================
#
# "Mid-scale" is not a field in TTS -- it is a judgment call we impose via a
# system-size range. The band below (100 kW to 5,000 kW DC) is the common
# commercial/community-solar convention, but it IS a choice. Change these two
# numbers and re-source the script to test how sensitive your findings are.
#
# Everything downstream reads from these variables, so you only change them here.

# Following NREL, which defines the midscale PV market as behind-the-meter
# systems between 100 kW and 2 MW, and consistent with industry "mid-market"
# usage (Sunrock, NuWatt), which puts the floor at 100 kW.
#   Heeter, Gagnon & Bird (2016), Expanding Midscale Solar, NREL/TP-6A20-65938
#   DOI: https://doi.org/10.2172/1326896
#   PDF: https://docs.nlr.gov/docs/fy16osti/65938.pdf
#   NREL is now the National Laboratory of the Rockies (NLR); *.nrel.gov is
#   dead with no redirect, so cite the DOI, not a host.
# See README.md for the full provenance and the alternatives considered.
MIDSCALE_MIN_KW <- 100      # lower bound, kW-DC (inclusive)
MIDSCALE_MAX_KW <- 2000     # upper bound, kW-DC (EXCLUSIVE -- NREL's 2 MW ceiling)
START_DATE      <- "2018-01-01"

# INTERVAL CONVENTION -- worth stating explicitly, because getting this wrong
# is a classic source of quiet double-counting.
#
# Every size band in this script is HALF-OPEN: [lower, upper). A system is in
# the band if size >= lower AND size < upper. So the mid-scale band is
# [100 kW, 2000 kW) -- a system at exactly 2,000 kW is NOT mid-scale, it is the
# first system in the 2-5 MW class.
#
# Using half-open intervals everywhere means the bands tile the number line
# perfectly: no value falls in two bands, and no value falls in none. Mixing
# <= and < across your buckets is how you end up with an out-of-range row
# showing up inside a table that is supposed to be mid-scale only.

# This string is pasted into the WHERE clause of most queries below. Building it
# once keeps the definition consistent across every result.
#
# Reading the SQL: PV_system_size_DC must fall in the band, the install date
# must be on/after our start, and the size must be a real value (not the -1
# missing code).

midscale_filter <- sprintf(
  "PV_system_size_DC >= %f
     AND PV_system_size_DC < %f
     AND PV_system_size_DC > 0
     AND installation_date >= DATE '%s'",
  MIDSCALE_MIN_KW, MIDSCALE_MAX_KW, START_DATE
)

# -----------------------------------------------------------------------------
# THE SIZE LADDER
# -----------------------------------------------------------------------------
# A reusable CASE expression that stamps every system with a size-class label.
# Defined once here and pasted into several queries below, so that every table
# in this script bins systems the SAME way. If you want different cut points,
# change them here and every size table updates together.
#
# The numeric prefixes ('01.', '02.', ...) exist so that ORDER BY size_bin sorts
# the bands in physical order. Without them SQL sorts alphabetically and you get
# '10. 2-5 MW' sitting between '01.' and '02.', which looks like nonsense.
#
# The cut points follow how the industry actually talks about these systems:
# residential (<20 kW), small commercial (20-100 kW), the mid-scale bands
# (100 kW-2 MW per NREL), the 2-5 MW gap, and utility-scale (5 MW+).

SIZE_BIN_SQL <- "
      CASE
        WHEN PV_system_size_DC <     5 THEN '01. under 5 kW'
        WHEN PV_system_size_DC <    10 THEN '02. 5-10 kW'
        WHEN PV_system_size_DC <    20 THEN '03. 10-20 kW'
        WHEN PV_system_size_DC <    50 THEN '04. 20-50 kW'
        WHEN PV_system_size_DC <   100 THEN '05. 50-100 kW'
        WHEN PV_system_size_DC <   250 THEN '06. 100-250 kW'
        WHEN PV_system_size_DC <   500 THEN '07. 250-500 kW'
        WHEN PV_system_size_DC <  1000 THEN '08. 500 kW-1 MW'
        WHEN PV_system_size_DC <  2000 THEN '09. 1-2 MW'
        WHEN PV_system_size_DC <  5000 THEN '10. 2-5 MW'
        ELSE                                '11. 5 MW and above'
      END"
# ^ CASE WHEN is evaluated TOP TO BOTTOM and stops at the first match. That is
#   why each line only needs an upper bound -- if a row reaches the '< 250'
#   line, we already know it is >= 100 because the '< 100' line didn't catch it.


# -----------------------------------------------------------------------------
# THE ZIP CODE FIELD IS MALFORMED -- NORMALISE BEFORE JOINING
# -----------------------------------------------------------------------------
# zip_code arrives in at least four incompatible shapes, and only about a third
# of it is directly joinable:
#
#   1,219,495  clean 5-digit                     "94103"
#   2,218,897  float artifact, ALL OF CALIFORNIA "65014.0"
#     247,690  4-digit, leading zero already lost "1001"  (should be "01001")
#      73,726  zip+4                             "05039-9602"
#          18  garbage: phone numbers, city names, tab characters, "0.0"
#
# Two of these deserve comment.
#
# THE '.0' SUFFIX is the float-conversion bug -- somewhere upstream the field was
# held as a number and stringified back. It affects every California record, so
# 56% of the file. Note this happened BEFORE the data reached us: forcing
# zip_code to VARCHAR in 00_csv_to_parquet.R protects the leading zeros that
# still exist, but cannot restore ones already destroyed.
#
# THE 4-DIGIT ZIPS are exactly that destruction, one stage upstream. They are
# concentrated in the Northeast, where every zip starts with 0:
#   MA 141,898 | CT 50,028 | VT 27,347 | ME 18,165 | NH 10,244
# Vermont has lost the zero on 27,347 of its 27,391 zips.
#
# The good news: all of it is recoverable. Strip the '.0', split off the +4,
# then zero-pad to five. That yields a valid 5-digit code for 3,760,730 of the
# 3,760,753 non-missing zips -- effectively 100%.
#
# USE ZIP5 FOR ANY JOIN to census, ACS, utility territory or geography files.
# Joining on raw zip_code silently drops California and most of New England.

# ZIP5_SQL -- the expression that does this -- is defined in
# 00_csv_to_parquet.R (SECTION 3b, shared definitions), so this script and
# 02_build_tts_systems.R normalise zips identically.


# -----------------------------------------------------------------------------
# ONE ROW IS NOT ALWAYS ONE PROJECT
# -----------------------------------------------------------------------------
# A single physical installation can appear as SEVERAL rows: phases of one build
# (multiple_phase_system = 1), later additions (expansion_system = True), or a PV
# system and a storage system added at the same address at different times.
#
# TTS_link_ID is the guide's key for this -- "systems installed at the same
# address". The flag below marks the FIRST row at each address so you can count
# distinct projects instead of rows. Rows without a link ID are always primary,
# since there is nothing to collapse them into.
#
# !! WHEN TO USE IT, AND WHEN NOT TO !!
#
#   COUNTING PROJECTS  -> use it. Mid-scale rows overstate distinct
#                         addresses; row counts overstate projects by ~3%.
#
#   SUMMING DOLLARS    -> DO NOT use it. Each phase row carries its OWN cost,
#   OR CAPACITY           not a repeat of the whole project's. Verified: of
#                         two-row groups, 390 of 430 have different sizes and
#                         225 of 287 different prices, and phased rows run
#                         $2.04/W against $2.17/W for single-row projects --
#                         consistent with incremental costs, not duplicates.
#                         Collapsing would DELETE real capital.
#
#   PERCENTAGES        -> barely matters. Companion rows almost always agree
#   AND SHARES            with each other (497 of 509 groups share one segment,
#                         474 of 509 one ownership value), so numerator and
#                         denominator shrink together. Third-party-owned share
#                         moves 28.5% -> 28.4%; no segment share moves more
#                         than 0.4 points. Pick one basis and stay consistent.
#
# The genuine duplicates are a much smaller set: 24 groups where rows share an
# identical link ID, size AND price to the cent -- 28 redundant rows carrying
# $25.6M, or 0.18% of reported mid-scale capital.
#
# -----------------------------------------------------------------------------
# HOW LONG APART IS STILL "THE SAME PROJECT"? (the time window)
# -----------------------------------------------------------------------------
# TTS_link_ID groups by ADDRESS, not by project, and address is broader. Among
# the 509 mid-scale groups holding more than one system:
#
#   177 were installed within a WEEK   -> unambiguously phases of one build
#    78 within 3 months
#    71 within a year
#    98 between 1 and 3 years apart    -> arguably a separate decision
#    85 more than 3 years apart        -> almost certainly a separate decision
#
# A school that adds a second array three years later is one ADDRESS but two
# financing events. Collapsing those hides a repeat customer -- which, for a
# capital-deployment question, is exactly the thing you want to see.
#
# So the window below is a JUDGMENT CALL you should make deliberately, not a
# property of the data. Systems at the same address are treated as one project
# only if they were installed within this many months of each other:
#
#   0     never collapse -- every system is its own project (= row counts)
#   12    a year: staged builds count once, later expansions count separately
#   999   collapse regardless of gap -- "same address, ever"
#
# Query 06f reports the project count at several windows so you can see how
# sensitive your answer is before committing. NOTE this affects COUNTS only --
# money and capacity are never de-duplicated, whatever you set here.

# DEDUP_WINDOW_MONTHS (default 12) and dedup_cte(), which builds the flag, are
# defined in 00_csv_to_parquet.R (SECTION 3b, shared definitions), so this
# script and 02_build_tts_systems.R group systems into projects identically.
# Change the window THERE.

# -----------------------------------------------------------------------------
# A CAVEAT YOU MUST CARRY INTO EVERY INTERPRETATION
# -----------------------------------------------------------------------------
#   TTS reports `total_installed_price` -- the INSTALLED COST of the system as
#   reported to a state incentive program or utility interconnection queue.
#
#   That is a proxy for capital deployed, not a measurement of it. It is not
#   the financing amount, not the equity check, not the tax-equity raise, and
#   it excludes soft costs the reporting program didn't capture. When you write
#   up "capital deployed," say "reported installed cost" and cite the coverage
#   rate that the queries below give you.
# =============================================================================


# =============================================================================
# SECTION 2 -- ORIENT YOURSELF: WHAT IS IN THIS FILE?
# =============================================================================

# --- 2.1 The column list ----------------------------------------------------
# DESCRIBE tells you every column name and its data type. Run this first
# whenever you come back to the dataset -- it is your map.

run("01_schema", "COLUMN LIST (94 columns)", "
  DESCRIBE SELECT * FROM tts
")

# --- 2.2 Overall size and time span -----------------------------------------
# Note the two ways of counting: count(*) counts rows; count(DISTINCT col)
# counts unique values in a column.

run("02_overview", "DATASET OVERVIEW (all records, all years)", "
  SELECT
    count(*)                        AS total_records,
    count(DISTINCT state)           AS states_covered,
    count(DISTINCT data_provider_1) AS data_providers,
    min(installation_date)::VARCHAR AS earliest_install,
    max(installation_date)::VARCHAR AS latest_install,
    -- systems with no usable size, i.e. the -1 missing code. These are dropped
    -- from every size-band table below, so it's worth knowing how many there are.
    count(*) FILTER (WHERE PV_system_size_DC <= 0) AS records_missing_size
  FROM tts
")

# --- 2.3 THE COVERAGE TABLE (the single most useful query here) -------------
# For the fields our research questions depend on, what fraction of MID-SCALE
# post-2018 records actually carry a usable value?
#
# The pattern below is: count how many rows pass a validity test, then divide
# by the total. `100.0 *` forces decimal (not integer) division; round(..., 1)
# trims it to one decimal place.
#
# Read the output as a go/no-go list. A field at 20% coverage cannot support a
# national conclusion; a field at 95% can.

run("03_field_coverage", "FIELD COVERAGE FOR MID-SCALE PROJECTS SINCE 2018", sprintf("
  SELECT
    count(*) AS midscale_systems,

    round(100.0 * count(*) FILTER (WHERE total_installed_price > 0)
          / count(*), 1) AS pct_with_price,

    round(100.0 * count(*) FILTER (WHERE rebate_or_grant > 0)
          / count(*), 1) AS pct_with_rebate,

    round(100.0 * count(*) FILTER (WHERE third_party_owned >= 0)
          / count(*), 1) AS pct_with_ownership,

    round(100.0 * count(*) FILTER (WHERE customer_segment NOT IN ('-1','redacted'))
          / count(*), 1) AS pct_with_segment,

    round(100.0 * count(*) FILTER (WHERE installer_name NOT IN ('-1','redacted'))
          / count(*), 1) AS pct_with_installer,

    round(100.0 * count(*) FILTER (WHERE ground_mounted >= 0)
          / count(*), 1) AS pct_with_mount_type,

    round(100.0 * count(*) FILTER (WHERE med_inc_HH_tract > 0)
          / count(*), 1) AS pct_with_tract_income
  FROM tts
  WHERE %s
", midscale_filter))

# --- 2.4 Geographic coverage: WHICH STATES ARE EVEN IN THIS FILE? -----------
# TTS is assembled from state incentive programs and utility interconnection
# data. It is NOT a national census -- roughly half the states are absent
# entirely, and that absence is the single biggest limit on any national claim
# you make. Print this table and keep it next to you.

run("04_state_coverage", "STATE COVERAGE -- MID-SCALE PROJECTS SINCE 2018", sprintf("
  SELECT
    state,
    count(*)                                              AS systems,
    round(sum(PV_system_size_DC) / 1000.0, 1)             AS total_mw_dc,
    count(*) FILTER (WHERE total_installed_price > 0)     AS n_with_price,
    round(100.0 * count(*) FILTER (WHERE total_installed_price > 0)
          / count(*), 1)                                  AS pct_with_price,
    min(installation_date)::VARCHAR                       AS first_install,
    max(installation_date)::VARCHAR                       AS last_install
  FROM tts
  WHERE %s
  GROUP BY state
  ORDER BY systems DESC
", midscale_filter))

# --- 2.5 Reporting lag: is the most recent year complete? -------------------
# Data providers submit on a lag, so the final year or two is ALWAYS
# undercounted. If you chart a time series without knowing this, you will
# report a fake collapse in the last year. This query shows you where the
# cliff is.

run("05_annual_completeness", "RECORDS PER INSTALL YEAR (check for reporting lag)", sprintf("
  SELECT
    year(installation_date)                            AS install_year,
    count(*)                                           AS systems,
    count(DISTINCT state)                              AS states_reporting,
    round(sum(PV_system_size_DC) / 1000.0, 1)          AS total_mw_dc
  FROM tts
  WHERE %s
  GROUP BY install_year
  ORDER BY install_year
", midscale_filter))

# --- 2.6 SYSTEM COUNTS BY kW-DC SIZE BAND -----------------------------------
# The core distribution table: how many systems sit in each size band, and how
# much capacity and capital each band represents.
#
# Read this table with one question in mind: SYSTEM COUNT AND CAPACITY TELL
# COMPLETELY DIFFERENT STORIES. Mid-scale is a rounding error by system count
# but a large share of the megawatts. Which denominator you choose will decide
# what your findings look like, so look at both columns before you pick one.
#
# Note we exclude PV_system_size_DC <= 0 (the -1 missing code). Leaving those in
# would put a negative number into the capacity sums and corrupt every
# percentage in the table.
#
# Three column groups to read:
#   n_systems / pct_systems     -- the count distribution
#   cum_systems / cum_pct       -- running totals, so you can say things like
#                                  "96.6% of systems are under 20 kW"
#   mw_dc / pct_mw              -- the capacity distribution (the other story)

run("06_size_bands", "SYSTEM COUNTS BY kW-DC SIZE BAND, ALL SYSTEMS SINCE 2018", sprintf("
  WITH binned AS (
    SELECT
      %s AS size_bin,
      PV_system_size_DC     AS kw,
      total_installed_price AS price
    FROM tts
    WHERE installation_date >= DATE '%s'
      AND PV_system_size_DC > 0
  )
  SELECT
    size_bin,
    count(*)                                              AS n_systems,
    round(100.0 * count(*) / sum(count(*)) OVER (), 2)    AS pct_systems,
    sum(count(*)) OVER (ORDER BY size_bin)                AS cum_systems,
    round(100.0 * sum(count(*)) OVER (ORDER BY size_bin)
          / sum(count(*)) OVER (), 1)                     AS cum_pct_systems,
    round(sum(kw) / 1000.0, 1)                            AS mw_dc,
    round(100.0 * sum(kw) / sum(sum(kw)) OVER (), 2)      AS pct_mw,
    round(median(kw), 1)                                  AS median_kw,
    count(*) FILTER (WHERE price > 0)                     AS n_priced,
    round(sum(price) FILTER (WHERE price > 0) / 1e6, 1)   AS capital_musd,
    round(sum(price) FILTER (WHERE price > 0)
          / NULLIF(sum(kw) FILTER (WHERE price > 0) * 1000.0, 0), 2)
                                                          AS usd_per_watt
  FROM binned
  GROUP BY size_bin
  ORDER BY size_bin
", SIZE_BIN_SQL, START_DATE))
# ^ Two window-function patterns worth learning here:
#
#   sum(count(*)) OVER ()                  -- grand total across ALL groups.
#       An aggregate wrapped in a window function. count(*) collapses each
#       group to a number; sum(...) OVER () then adds those numbers up. This
#       is how you compute "share of total" without a second query.
#
#   sum(count(*)) OVER (ORDER BY size_bin) -- RUNNING total.
#       Adding ORDER BY inside OVER() turns it into a cumulative sum: each row
#       gets the total of itself plus every row before it in that order.

# --- 2.7 Are systems getting bigger? Size bands by year ---------------------
# Same bands, now crossed with install year, restricted to mid-scale. This is
# where you see whether the mid-scale market is shifting toward larger or
# smaller projects over time -- a different question from whether it is growing.

run("06b_size_bands_by_year", "MID-SCALE SYSTEM COUNTS BY SIZE BAND AND YEAR", sprintf("
  WITH binned AS (
    SELECT
      %s AS size_bin,
      year(installation_date) AS yr,
      PV_system_size_DC       AS kw
    FROM tts
    WHERE %s
  )
  SELECT
    size_bin,
    count(*) FILTER (WHERE yr = 2018) AS y2018,
    count(*) FILTER (WHERE yr = 2019) AS y2019,
    count(*) FILTER (WHERE yr = 2020) AS y2020,
    count(*) FILTER (WHERE yr = 2021) AS y2021,
    count(*) FILTER (WHERE yr = 2022) AS y2022,
    count(*) FILTER (WHERE yr = 2023) AS y2023,
    count(*) FILTER (WHERE yr = 2024) AS y2024,
    count(*) FILTER (WHERE yr = 2025) AS y2025,
    count(*)                          AS total_systems,
    round(sum(kw) / 1000.0, 1)        AS total_mw_dc
  FROM binned
  GROUP BY size_bin
  ORDER BY size_bin
", SIZE_BIN_SQL, midscale_filter))

# --- 2.8 Size bands by state ------------------------------------------------
# Where each state's mid-scale market actually sits on the size ladder. States
# differ enormously here, and the reason is usually policy: program capacity
# caps, net-metering thresholds and community-solar rules all pin projects to
# specific sizes. A state whose median lands right under a round number is
# almost certainly being shaped by a cap at that number.

run("06c_size_bands_by_state", "MID-SCALE SYSTEM COUNTS BY SIZE BAND AND STATE", sprintf("
  WITH binned AS (
    SELECT %s AS size_bin, state, PV_system_size_DC AS kw
    FROM tts
    WHERE %s
  )
  SELECT
    state,
    count(*) FILTER (WHERE size_bin = '06. 100-250 kW')  AS kw100_250,
    count(*) FILTER (WHERE size_bin = '07. 250-500 kW')  AS kw250_500,
    count(*) FILTER (WHERE size_bin = '08. 500 kW-1 MW') AS kw500_1mw,
    count(*) FILTER (WHERE size_bin = '09. 1-2 MW')      AS mw1_2,
    count(*)                                             AS total_systems,
    round(median(kw), 1)                                 AS median_kw,
    round(sum(kw) / 1000.0, 1)                           AS total_mw_dc
  FROM binned
  GROUP BY state
  HAVING count(*) >= 25
  ORDER BY total_systems DESC
", SIZE_BIN_SQL, midscale_filter))

# --- 2.9 Size bands by customer segment -------------------------------------
# What size system does each type of host actually build? Useful for sizing an
# investment thesis -- a strategy aimed at 1-2 MW projects is fishing in a very
# different segment pool than one aimed at 100-250 kW.

run("06d_size_bands_by_segment", "MID-SCALE SYSTEM COUNTS BY SIZE BAND AND SEGMENT", sprintf("
  WITH binned AS (
    SELECT %s AS size_bin, customer_segment AS segment,
           PV_system_size_DC AS kw, total_installed_price AS price
    FROM tts
    WHERE %s
  )
  SELECT
    segment,
    count(*) FILTER (WHERE size_bin = '06. 100-250 kW')  AS kw100_250,
    count(*) FILTER (WHERE size_bin = '07. 250-500 kW')  AS kw250_500,
    count(*) FILTER (WHERE size_bin = '08. 500 kW-1 MW') AS kw500_1mw,
    count(*) FILTER (WHERE size_bin = '09. 1-2 MW')      AS mw1_2,
    count(*)                                             AS total_systems,
    round(median(kw), 1)                                 AS median_kw,
    round(sum(price) FILTER (WHERE price > 0) / 1e6, 1)  AS capital_musd
  FROM binned
  GROUP BY segment
  ORDER BY total_systems DESC
", SIZE_BIN_SQL, midscale_filter))

# --- 2.10 !! DATA-QUALITY FLAG: 'RES' DOES NOT MEAN RESIDENTIAL AT THIS SIZE --
#
# Look at the median_kw column in the table above. The 'RES' segment shows a
# median around 1,278 kW -- a 1.3 MW "residential" system, which is absurd on
# its face. A real residential rooftop is 5-10 kW (see query 06).
#
# WHAT IS ACTUALLY GOING ON
#   These are COMMUNITY SOLAR projects. In several state programs the
#   interconnection record is coded by the SUBSCRIBER class rather than by the
#   host: a 1.3 MW solar garden whose subscribers are households gets filed as
#   'RES'. The customer_segment field is describing who buys the power, not
#   what was built.
#
#   Minnesota is the clearest case and the largest. Every one of its mid-scale
#   records sits in Xcel Energy territory -- the Solar*Rewards Community
#   program -- and the sizes pile up at 1,300-1,500 kW because the program caps
#   gardens at 1 MW AC (roughly 1.3-1.5 MW DC). That is a regulatory ceiling
#   showing up in the data, not a market preference.
#
# WHY YOU MUST HANDLE THIS BEFORE ANALYZING
#   1. It contaminates the segment split. Minnesota alone is ~44% of all
#      'RES' mid-scale records nationally. Any statement about "residential
#      mid-scale" is mostly a statement about Midwestern community solar.
#   2. It distorts the size distribution. MN is ~33% of the entire national
#      1-2 MW band. Query 06b's 1-2 MW row is largely one state's program.
#   3. It skews capacity but NOT capital. Almost none of these records carry a
#      price (MN: 10 of 664) or an ownership flag (MN: 0 of 664). So they add
#      megawatts to your totals while contributing nothing to the dollar
#      figures -- which quietly widens the gap between reported and scaled
#      capital in query 07.
#
# The query below surfaces every state where this is happening so you can
# decide what to do with them. Read the median_kw column: anything in the
# hundreds or thousands of kW is community solar wearing a residential label.

run("06e_res_label_check", "!! FLAG -- 'RES'-LABELED MID-SCALE (LIKELY COMMUNITY SOLAR)", sprintf("
  SELECT
    state,
    count(*)                                              AS res_labeled_systems,
    round(median(PV_system_size_DC), 1)                   AS median_kw,
    round(sum(PV_system_size_DC) / 1000.0, 1)             AS mw_dc,
    count(*) FILTER (WHERE total_installed_price > 0)     AS n_with_price,
    count(*) FILTER (WHERE third_party_owned >= 0)        AS n_with_ownership,
    count(DISTINCT utility_service_territory)             AS n_utilities,
    -- a median in the hundreds/thousands of kW cannot be a rooftop
    CASE WHEN median(PV_system_size_DC) >= 500
         THEN 'ALMOST CERTAINLY COMMUNITY SOLAR'
         ELSE 'check -- may be multifamily/shared' END    AS interpretation
  FROM tts
  WHERE %s
    AND customer_segment = 'RES'
  GROUP BY state
  ORDER BY res_labeled_systems DESC
", midscale_filter))

# --- 2.10b !! DATA-QUALITY FLAG: THE ZIP FIELD ------------------------------
# Counts the shapes zip_code actually arrives in, file-wide, and confirms the
# normalisation recovers them. Run this against any new release before trusting
# a geographic join -- the mix of formats is a property of who reported, so it
# can change when a state's data provider changes.
#
# Read `pct_valid_after` at the bottom: if it is not ~100%, ZIP5_SQL needs
# another case adding before you join to anything.

run("06h_zip_format_check", "!! FLAG -- ZIP CODE FORMATS (file-wide)", sprintf("
  SELECT
    CASE
      WHEN zip_code IN ('-1', 'redacted')                THEN '0. missing'
      WHEN regexp_matches(zip_code, '^[0-9]{5}$')        THEN '1. clean 5-digit'
      WHEN zip_code LIKE '%%.0'                          THEN '2. float artifact (.0)'
      WHEN regexp_matches(zip_code, '^[0-9]{4}$')        THEN '3. 4-digit, zero lost'
      WHEN regexp_matches(zip_code, '^[0-9]{5}-[0-9]{4}$') THEN '4. zip+4'
      ELSE                                                    '5. other / malformed'
    END                                                       AS zip_format,
    count(*)                                                  AS records,
    round(100.0 * count(*) / sum(count(*)) OVER (), 2)        AS pct,
    count(DISTINCT state)                                     AS states,
    min(zip_code)                                             AS example
  FROM tts
  GROUP BY zip_format
  ORDER BY zip_format
"))
# ^ '%%.0' not '%.0' -- sprintf() needs the percent sign escaped.

run("06i_zip5_recovery", "ZIP NORMALISATION -- DOES IT RECOVER EVERYTHING?", sprintf("
  WITH z AS (SELECT %s AS zip5 FROM tts WHERE zip_code NOT IN ('-1', 'redacted'))
  SELECT
    count(*)                                                        AS non_missing,
    count(*) FILTER (WHERE regexp_matches(zip5, '^[0-9]{5}$'))      AS valid_after,
    count(*) - count(*) FILTER (WHERE regexp_matches(zip5, '^[0-9]{5}$'))
                                                                    AS still_broken,
    round(100.0 * count(*) FILTER (WHERE regexp_matches(zip5, '^[0-9]{5}$'))
          / count(*), 2)                                            AS pct_valid_after
  FROM z
", ZIP5_SQL))

# --- 2.11 ROWS vs DISTINCT PROJECTS ----------------------------------------
# How much do row counts overstate the number of real projects, and does it
# change the segment mix or the ownership split? Read the `removed` column to
# see how many companion rows each segment carries, then compare pct_rows
# against pct_projects to see whether it actually moves the answer.
#
# Spoiler, so you can budget your attention: it moves counts by ~3% and shares
# by under half a point. Use de-duplicated counts when you say "how many
# projects"; don't bother re-basing percentages, and never de-duplicate money.

run("06f_rows_vs_projects", sprintf(
  "SYSTEMS vs PROJECTS BY SEGMENT (window = %d months)", DEDUP_WINDOW_MONTHS),
  paste0(dedup_cte(midscale_filter), "
  SELECT
    customer_segment                                            AS segment,
    count(*)                                                    AS systems,
    count(*) FILTER (WHERE is_first_at_address)                 AS projects,
    count(*) - count(*) FILTER (WHERE is_first_at_address)      AS collapsed,

    round(100.0 * count(*) / sum(count(*)) OVER (), 1)          AS pct_systems,
    round(100.0 * count(*) FILTER (WHERE is_first_at_address)
          / sum(count(*) FILTER (WHERE is_first_at_address)) OVER (), 1)
                                                                AS pct_projects,

    -- ownership share on each basis, to show it barely moves
    round(100.0 * count(*) FILTER (WHERE third_party_owned = 1)
          / NULLIF(count(*) FILTER (WHERE third_party_owned >= 0), 0), 1)
                                                                AS pct_tpo_systems,
    round(100.0 * count(*) FILTER (WHERE third_party_owned = 1 AND is_first_at_address)
          / NULLIF(count(*) FILTER (WHERE third_party_owned >= 0 AND is_first_at_address), 0), 1)
                                                                AS pct_tpo_projects,

    -- capital is deliberately NOT de-duplicated; see Section 1
    round(sum(total_installed_price) FILTER (WHERE total_installed_price > 0)
          / 1e6, 1)                                             AS capital_musd
  FROM flagged
  GROUP BY segment
  ORDER BY systems DESC"))

# --- 2.12 HOW SENSITIVE IS THE PROJECT COUNT TO THE WINDOW? -----------------
# Runs the same count at several windows so you can see whether your choice
# actually matters before defending it. Read the two ends first: 0 months is
# the raw system count, 999 months collapses any two systems that ever shared
# an address. If those are close, the decision is not worth agonising over.
#
# This loops in R rather than doing it in one query -- clearer than a UNION of
# six near-identical SELECTs, and the loop makes the varying part obvious.

windows <- c(0, 1, 3, 12, 36, 999)

sensitivity <- do.call(rbind, lapply(windows, function(w) {
  res <- q(paste0(dedup_cte(midscale_filter, window_months = w), "
    SELECT count(*) AS systems,
           count(*) FILTER (WHERE is_first_at_address) AS projects
    FROM flagged"))
  data.frame(
    window_months   = if (w == 0) "0 (none)" else if (w >= 999) "999 (any gap)" else as.character(w),
    systems         = res$systems,
    projects        = res$projects,
    collapsed       = res$systems - res$projects,
    pct_collapsed   = round(100 * (res$systems - res$projects) / res$systems, 2)
  )
}))

cat("\n\n========================================================\n")
cat("SENSITIVITY OF THE PROJECT COUNT TO THE TIME WINDOW\n")
cat("========================================================\n")
print(sensitivity, row.names = FALSE, right = FALSE)
write.csv(sensitivity, file.path(output_dir, "06g_dedup_window_sensitivity.csv"),
          row.names = FALSE)


# =============================================================================
# SECTION 3 -- RQ0: HOW MUCH CAPITAL HAS BEEN DEPLOYED SINCE 2018?
# =============================================================================

# --- 3.1 Headline totals ----------------------------------------------------
# Two numbers matter here and they are NOT the same:
#
#   reported_capital  = sum of prices we actually observe. An UNDERCOUNT,
#                       because ~1/3 of projects report no price.
#   scaled_estimate   = reported capital grossed up by the inverse of the
#                       coverage rate. Better, but it assumes projects missing
#                       a price cost the same per watt as those reporting one.
#                       That assumption is untested. Report both.
#
# `sum(x) FILTER (WHERE ...)` sums only the qualifying rows.

run("07_capital_headline", "RQ0 -- REPORTED CAPITAL, MID-SCALE, SINCE 2018", sprintf("
  SELECT
    count(*)                                                 AS midscale_systems,
    count(*) FILTER (WHERE total_installed_price > 0)         AS systems_with_price,
    round(sum(PV_system_size_DC) / 1000.0, 1)                AS total_mw_dc,

    round(sum(total_installed_price) FILTER (WHERE total_installed_price > 0)
          / 1e6, 1)                                          AS reported_capital_musd,

    -- capacity that actually sits behind the reported dollars
    round(sum(PV_system_size_DC) FILTER (WHERE total_installed_price > 0)
          / 1000.0, 1)                                       AS priced_mw_dc,

    -- implied $/W-DC: dollars / (kW * 1000)
    round(sum(total_installed_price) FILTER (WHERE total_installed_price > 0)
          / (sum(PV_system_size_DC) FILTER (WHERE total_installed_price > 0) * 1000.0),
          2)                                                 AS implied_usd_per_watt,

    -- gross-up: total MW * observed $/W. Treat as an ESTIMATE, not a fact.
    round(sum(PV_system_size_DC) * 1000.0
          * (sum(total_installed_price) FILTER (WHERE total_installed_price > 0)
             / (sum(PV_system_size_DC) FILTER (WHERE total_installed_price > 0) * 1000.0))
          / 1e6, 1)                                          AS scaled_estimate_musd
  FROM tts
  WHERE %s
", midscale_filter))

# --- 3.2 The same, by year --------------------------------------------------
# Use this for your trend chart. Remember Section 2.5: discount the final year.

run("08_capital_by_year", "RQ0 -- REPORTED CAPITAL BY YEAR", sprintf("
  SELECT
    year(installation_date)                                  AS install_year,
    count(*)                                                 AS systems,
    round(sum(PV_system_size_DC) / 1000.0, 1)                AS mw_dc,
    count(*) FILTER (WHERE total_installed_price > 0)         AS n_priced,
    round(sum(total_installed_price) FILTER (WHERE total_installed_price > 0)
          / 1e6, 1)                                          AS reported_capital_musd,
    round(median(NULLIF(total_installed_price, -1)) , 0)     AS median_project_usd,
    round(sum(total_installed_price) FILTER (WHERE total_installed_price > 0)
          / (sum(PV_system_size_DC) FILTER (WHERE total_installed_price > 0) * 1000.0),
          2)                                                 AS usd_per_watt
  FROM tts
  WHERE %s
  GROUP BY install_year
  ORDER BY install_year
", midscale_filter))


# =============================================================================
# SECTION 4 -- RQ1: WHAT PROJECT CATEGORIES HAVE ATTRACTED INVESTMENT?
# =============================================================================
#
# TTS gives us three usable category dimensions:
#   customer_segment  -- who the offtaker is (COM, SCHOOL, GOV, AGRICULTURAL...)
#   technology_type   -- pv-only vs pv+storage vs storage-only
#   ground_mounted    -- rooftop vs ground-mount (a rough proxy for project type)
#
# It does NOT tell us community solar vs. behind-the-meter directly. That is a
# real gap; note it.

run("09_by_customer_segment", "RQ1 -- CAPITAL BY CUSTOMER SEGMENT", sprintf("
  SELECT
    customer_segment,
    count(*)                                                 AS systems,
    round(sum(PV_system_size_DC) / 1000.0, 1)                AS mw_dc,
    round(avg(PV_system_size_DC), 0)                         AS avg_kw,
    count(*) FILTER (WHERE total_installed_price > 0)         AS n_priced,
    round(sum(total_installed_price) FILTER (WHERE total_installed_price > 0)
          / 1e6, 1)                                          AS reported_capital_musd,
    round(sum(total_installed_price) FILTER (WHERE total_installed_price > 0)
          / (sum(PV_system_size_DC) FILTER (WHERE total_installed_price > 0) * 1000.0),
          2)                                                 AS usd_per_watt
  FROM tts
  WHERE %s
  GROUP BY customer_segment
  ORDER BY reported_capital_musd DESC NULLS LAST
", midscale_filter))

run("10_by_technology", "RQ1 -- STORAGE ATTACHMENT AND MOUNT TYPE", sprintf("
  SELECT
    technology_type,
    -- decode the numeric flag into something readable
    CASE ground_mounted WHEN 1 THEN 'ground-mount'
                        WHEN 0 THEN 'rooftop'
                        ELSE 'unknown' END                   AS mount_type,
    count(*)                                                 AS systems,
    round(sum(PV_system_size_DC) / 1000.0, 1)                AS mw_dc,
    round(sum(total_installed_price) FILTER (WHERE total_installed_price > 0)
          / 1e6, 1)                                          AS reported_capital_musd,
    round(avg(NULLIF(battery_rated_capacity_kWh, -1)), 1)    AS avg_battery_kwh
  FROM tts
  WHERE %s
  GROUP BY technology_type, mount_type
  ORDER BY systems DESC
", midscale_filter))

# Storage attachment rate over time -- the fastest-moving structural trend in
# this dataset, and directly relevant to where new capital is going.
run("11_storage_trend", "RQ1 -- STORAGE ATTACHMENT RATE BY YEAR", sprintf("
  SELECT
    year(installation_date)                                  AS install_year,
    count(*)                                                 AS systems,
    count(*) FILTER (WHERE technology_type = 'pv+storage')    AS n_with_storage,
    round(100.0 * count(*) FILTER (WHERE technology_type = 'pv+storage')
          / count(*), 1)                                     AS pct_with_storage
  FROM tts
  WHERE %s
  GROUP BY install_year
  ORDER BY install_year
", midscale_filter))


# =============================================================================
# SECTION 5 -- RQ2: WHAT FINANCING STRUCTURES ARE MOST PREVALENT?
# =============================================================================
#
# READ THIS BEFORE USING THE OUTPUT.
#
# TTS has four ownership/finance fields, and they are NOT equally trustworthy:
#
#   third_party_owned            1 = TPO (lease or PPA), 0 = host-owned, -1 = unknown
#   third_party_owned_inferred   LBNL's model-filled version of the above
#   loan_inferred                1 = likely loan-financed  (INFERRED, not reported)
#   cash_inferred                1 = likely cash purchase  (INFERRED, not reported)
#
# The *_inferred fields are LBNL's statistical guesses, each carrying a
# companion *_confidence column with values H / M / L / redacted. The query
# below deliberately breaks results out BY confidence level so you can see how
# much of the signal rests on low-confidence imputation before you cite it.
#
# Note also: this dataset was designed around distributed/residential solar.
# Loan-vs-cash inference is far more meaningful for a $25k rooftop system than
# for a $2M commercial array, which is likelier to be financed through
# structures TTS simply does not observe (tax equity, C-PACE, project debt).
# For mid-scale, treat TPO-vs-host-owned as the reliable split and treat
# loan/cash as weak signal.

run("12_ownership_structure", "RQ2 -- OWNERSHIP STRUCTURE (reported, most reliable)", sprintf("
  SELECT
    CASE third_party_owned WHEN 1 THEN 'third-party owned (lease/PPA)'
                           WHEN 0 THEN 'host owned'
                           ELSE 'not reported' END           AS ownership,
    count(*)                                                 AS systems,
    round(100.0 * count(*) / sum(count(*)) OVER (), 1)       AS pct_of_projects,
    round(sum(PV_system_size_DC) / 1000.0, 1)                AS mw_dc,
    round(sum(total_installed_price) FILTER (WHERE total_installed_price > 0)
          / 1e6, 1)                                          AS reported_capital_musd,
    round(avg(PV_system_size_DC), 0)                         AS avg_kw
  FROM tts
  WHERE %s
  GROUP BY ownership
  ORDER BY systems DESC
", midscale_filter))
# ^ `sum(count(*)) OVER ()` is a WINDOW FUNCTION: it computes the grand total
#   across all groups so each row can show its own share of the whole. The
#   empty OVER () means "over the entire result set, no partitioning."

run("13_ownership_by_year", "RQ2 -- TPO SHARE OVER TIME", sprintf("
  SELECT
    year(installation_date)                                  AS install_year,
    count(*) FILTER (WHERE third_party_owned >= 0)            AS n_with_data,
    round(100.0 * count(*) FILTER (WHERE third_party_owned = 1)
          / NULLIF(count(*) FILTER (WHERE third_party_owned >= 0), 0), 1)
                                                             AS pct_tpo,
    round(100.0 * count(*) FILTER (WHERE third_party_owned = 0)
          / NULLIF(count(*) FILTER (WHERE third_party_owned >= 0), 0), 1)
                                                             AS pct_host_owned
  FROM tts
  WHERE %s
  GROUP BY install_year
  ORDER BY install_year
", midscale_filter))
# ^ NULLIF(denominator, 0) is the standard guard against divide-by-zero: if the
#   denominator is 0 it becomes NULL and the result is NULL rather than an error.

run("14_inferred_finance", "RQ2 -- INFERRED LOAN/CASH, BROKEN OUT BY CONFIDENCE", sprintf("
  SELECT
    loan_inferred_confidence                                 AS confidence,
    count(*)                                                 AS systems,
    count(*) FILTER (WHERE loan_inferred = 1)                 AS n_loan,
    count(*) FILTER (WHERE cash_inferred = 1)                 AS n_cash,
    count(*) FILTER (WHERE third_party_owned_inferred = 1)    AS n_tpo_inferred,
    round(sum(total_installed_price) FILTER (WHERE total_installed_price > 0)
          / 1e6, 1)                                          AS reported_capital_musd
  FROM tts
  WHERE %s
  GROUP BY confidence
  ORDER BY systems DESC
", midscale_filter))

# Public subsidy alongside private capital -- how much of the cost was covered
# by a rebate or grant, where one is recorded?
run("15_rebates", "RQ2 -- REBATE / GRANT SUPPORT BY YEAR", sprintf("
  SELECT
    year(installation_date)                                  AS install_year,
    count(*) FILTER (WHERE rebate_or_grant > 0)               AS n_with_rebate,
    round(100.0 * count(*) FILTER (WHERE rebate_or_grant > 0)
          / count(*), 1)                                     AS pct_with_rebate,
    round(sum(rebate_or_grant) FILTER (WHERE rebate_or_grant > 0)
          / 1e6, 2)                                          AS rebate_dollars_musd,
    -- among projects reporting BOTH, what share of cost did the rebate cover?
    round(100.0 * sum(rebate_or_grant) FILTER (WHERE rebate_or_grant > 0 AND total_installed_price > 0)
          / NULLIF(sum(total_installed_price) FILTER (WHERE rebate_or_grant > 0 AND total_installed_price > 0), 0),
          1)                                                 AS rebate_pct_of_cost
  FROM tts
  WHERE %s
  GROUP BY install_year
  ORDER BY install_year
", midscale_filter))


# =============================================================================
# SECTION 6 -- RQ3: WHAT TYPES OF INVESTORS HAVE PARTICIPATED?
# =============================================================================
#
# BLUNT ANSWER: TTS does not identify investors. There is no capital-provider
# field, no sponsor, no lender, no tax-equity partner.
#
# The closest available proxies are:
#   installer_name  -- the developer/EPC, which for TPO projects often IS the
#                      capital sponsor or its affiliate
#   third_party_owned -- distinguishes "someone else's balance sheet" from
#                      "the host's own balance sheet"
#
# Use the query below to identify WHO IS BUILDING, then plan to join to an
# external source (e.g. Wood Mackenzie, S&P, EDGAR, state PUC filings) to learn
# who is FUNDING. Treat installer concentration as a market-structure finding,
# not an investor finding.

run("16_top_installers", "RQ3 (PROXY) -- TOP DEVELOPERS BY REPORTED CAPITAL", sprintf("
  SELECT
    installer_name,
    count(*)                                                 AS systems,
    count(DISTINCT state)                                    AS states_active,
    round(sum(PV_system_size_DC) / 1000.0, 1)                AS mw_dc,
    round(sum(total_installed_price) FILTER (WHERE total_installed_price > 0)
          / 1e6, 1)                                          AS reported_capital_musd,
    round(100.0 * count(*) FILTER (WHERE third_party_owned = 1)
          / NULLIF(count(*) FILTER (WHERE third_party_owned >= 0), 0), 1)
                                                             AS pct_tpo
  FROM tts
  WHERE %s
    AND installer_name NOT IN ('-1', 'redacted')
  GROUP BY installer_name
  HAVING count(*) >= 5              -- drop one-off installers from the ranking
  ORDER BY reported_capital_musd DESC NULLS LAST
  LIMIT 30
", midscale_filter))
# ^ HAVING vs WHERE: WHERE filters individual rows BEFORE grouping; HAVING
#   filters the GROUPS after. You cannot use count(*) in a WHERE clause.

# Market concentration: is mid-scale dominated by a few players, or fragmented?
# This matters for the "underserved markets" question -- a state served by one
# installer is fragile.
run("17_installer_concentration", "RQ3 -- DEVELOPER CONCENTRATION BY STATE", sprintf("
  SELECT
    state,
    count(*)                                                 AS systems,
    count(DISTINCT installer_name) FILTER (WHERE installer_name NOT IN ('-1','redacted'))
                                                             AS distinct_installers,
    round(1.0 * count(*)
          / NULLIF(count(DISTINCT installer_name) FILTER (WHERE installer_name NOT IN ('-1','redacted')), 0),
          1)                                                 AS systems_per_installer
  FROM tts
  WHERE %s
  GROUP BY state
  HAVING count(*) >= 25
  ORDER BY systems_per_installer DESC
", midscale_filter))


# =============================================================================
# SECTION 7 -- RQ4: WHICH REGIONS AND SEGMENTS HAVE GROWN MOST?
# =============================================================================

# --- 7.1 Growth by state: first period vs. most recent period ---------------
# This compares two multi-year windows rather than single years, which smooths
# out the reporting lag problem. Growth on a small base is noisy, so the
# HAVING clause requires a minimum project count.

run("18_state_growth", "RQ4 -- STATE GROWTH: 2018-2020 vs 2022-2024", sprintf("
  SELECT
    state,
    round(sum(PV_system_size_DC) FILTER (WHERE year(installation_date) BETWEEN 2018 AND 2020)
          / 1000.0, 1)                                       AS mw_2018_2020,
    round(sum(PV_system_size_DC) FILTER (WHERE year(installation_date) BETWEEN 2022 AND 2024)
          / 1000.0, 1)                                       AS mw_2022_2024,
    round(100.0 *
      (sum(PV_system_size_DC) FILTER (WHERE year(installation_date) BETWEEN 2022 AND 2024)
       - sum(PV_system_size_DC) FILTER (WHERE year(installation_date) BETWEEN 2018 AND 2020))
      / NULLIF(sum(PV_system_size_DC) FILTER (WHERE year(installation_date) BETWEEN 2018 AND 2020), 0),
      1)                                                     AS pct_change_mw,
    count(*)                                                 AS total_projects
  FROM tts
  WHERE %s
  GROUP BY state
  HAVING count(*) >= 25
  ORDER BY pct_change_mw DESC NULLS LAST
", midscale_filter))

# --- 7.2 Growth by segment --------------------------------------------------
run("19_segment_growth", "RQ4 -- SEGMENT GROWTH: 2018-2020 vs 2022-2024", sprintf("
  SELECT
    customer_segment,
    count(*) FILTER (WHERE year(installation_date) BETWEEN 2018 AND 2020) AS n_2018_2020,
    count(*) FILTER (WHERE year(installation_date) BETWEEN 2022 AND 2024) AS n_2022_2024,
    round(sum(PV_system_size_DC) FILTER (WHERE year(installation_date) BETWEEN 2018 AND 2020)
          / 1000.0, 1)                                       AS mw_2018_2020,
    round(sum(PV_system_size_DC) FILTER (WHERE year(installation_date) BETWEEN 2022 AND 2024)
          / 1000.0, 1)                                       AS mw_2022_2024
  FROM tts
  WHERE %s
  GROUP BY customer_segment
  ORDER BY mw_2022_2024 DESC NULLS LAST
", midscale_filter))

# --- 7.3 Where the activity actually sits: state x segment ------------------
# A two-dimensional cross-tab. Empty or thin cells here ARE your market gaps.
run("20_state_by_segment", "RQ4 -- STATE x SEGMENT MATRIX (top cells)", sprintf("
  SELECT
    state,
    customer_segment,
    count(*)                                                 AS systems,
    round(sum(PV_system_size_DC) / 1000.0, 1)                AS mw_dc,
    round(sum(total_installed_price) FILTER (WHERE total_installed_price > 0)
          / 1e6, 1)                                          AS reported_capital_musd
  FROM tts
  WHERE %s
  GROUP BY state, customer_segment
  HAVING count(*) >= 10
  ORDER BY mw_dc DESC
  LIMIT 40
", midscale_filter))

# --- 7.4 Utility territory -- the sub-state unit that actually drives policy -
# Interconnection rules, net-metering terms and queue times are set at the
# utility level, not the state level. This is often where the real story is.
run("21_utility_territory", "RQ4 -- TOP UTILITY SERVICE TERRITORIES", sprintf("
  SELECT
    state,
    utility_service_territory,
    count(*)                                                 AS systems,
    round(sum(PV_system_size_DC) / 1000.0, 1)                AS mw_dc,
    round(sum(total_installed_price) FILTER (WHERE total_installed_price > 0)
          / 1e6, 1)                                          AS reported_capital_musd
  FROM tts
  WHERE %s
    AND utility_service_territory NOT IN ('-1', 'redacted')
  GROUP BY state, utility_service_territory
  ORDER BY mw_dc DESC
  LIMIT 30
", midscale_filter))


# =============================================================================
# SECTION 8 -- RQ5: WHERE ARE THE INVESTMENT GAPS?
# =============================================================================
#
# Three distinct kinds of "gap," and it is important not to conflate them:
#
#   (a) DATA GAPS      -- states/fields TTS simply doesn't cover. An absence of
#                         evidence, not evidence of absence.
#   (b) MARKET GAPS    -- places TTS covers well that still show little
#                         mid-scale activity. This is a real finding.
#   (c) EQUITY GAPS    -- who is being served within active markets, measured
#                         via the census-tract median income fields.

# --- 8.1 (a) DATA GAPS: which states are missing entirely? ------------------
# We can't query rows that don't exist, so we build a literal list of all 50
# states in R, hand it to DuckDB, and LEFT JOIN the data onto it. A LEFT JOIN
# keeps every row from the left-hand table even when there's no match on the
# right -- which is exactly how absent states become visible.

all_states <- c("AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL",
                "IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT",
                "NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI",
                "SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC")

# Turn the R vector into a SQL VALUES list: ('AL'),('AK'),...
state_values <- paste0("('", all_states, "')", collapse = ",")

run("22_missing_states", "RQ5(a) -- STATES ABSENT OR THIN IN TTS", sprintf("
  WITH us(state) AS (VALUES %s),
       cov AS (
         SELECT state,
                count(*)                        AS systems,
                round(sum(PV_system_size_DC)/1000.0, 1) AS mw_dc
         FROM tts
         WHERE %s
         GROUP BY state
       )
  SELECT
    us.state,
    coalesce(cov.systems, 0)                   AS midscale_systems,
    coalesce(cov.mw_dc, 0)                      AS mw_dc,
    CASE
      WHEN cov.systems IS NULL   THEN 'NO DATA -- state absent from TTS'
      WHEN cov.systems < 25      THEN 'THIN -- too few records to analyze'
      ELSE                             'covered'
    END                                         AS coverage_status
  FROM us
  LEFT JOIN cov ON us.state = cov.state
  ORDER BY midscale_systems ASC, us.state
", state_values, midscale_filter))
# ^ Two new pieces of SQL here:
#   WITH ... AS (...)  -- a "CTE" (common table expression): a named temporary
#                         result you can reference later in the same query.
#                         Think of it as a variable holding a table. It makes
#                         long queries readable instead of deeply nested.
#   coalesce(a, b)     -- returns a, unless a is NULL, in which case returns b.
#                         Here it turns "no match" into 0.

# --- 8.2 (b) MARKET GAPS: covered states with weak mid-scale activity -------
# Compares each state's mid-scale share against its overall solar activity. A
# state with lots of residential solar but almost no mid-scale has a structural
# barrier worth investigating (interconnection, net-metering caps, siting).

run("23_midscale_penetration", "RQ5(b) -- MID-SCALE SHARE OF EACH STATE'S SOLAR MARKET", sprintf("
  SELECT
    state,
    count(*)                                                 AS all_projects,
    -- NOTE: >= lower AND < upper, matching the half-open convention set in
    -- Section 1. Do not switch this to BETWEEN -- BETWEEN is inclusive on BOTH
    -- ends, which would double-count a system sitting exactly on a boundary.
    count(*) FILTER (WHERE PV_system_size_DC >= %f AND PV_system_size_DC < %f)
                                                             AS midscale_systems,
    round(100.0 * count(*) FILTER (WHERE PV_system_size_DC >= %f AND PV_system_size_DC < %f)
          / count(*), 2)                                     AS pct_midscale_by_count,
    round(100.0 * sum(PV_system_size_DC) FILTER (WHERE PV_system_size_DC >= %f AND PV_system_size_DC < %f)
          / NULLIF(sum(PV_system_size_DC) FILTER (WHERE PV_system_size_DC > 0), 0), 1)
                                                             AS pct_midscale_by_mw
  FROM tts
  WHERE installation_date >= DATE '%s'
    AND PV_system_size_DC > 0
  GROUP BY state
  HAVING count(*) >= 500
  ORDER BY pct_midscale_by_mw ASC
", MIDSCALE_MIN_KW, MIDSCALE_MAX_KW, MIDSCALE_MIN_KW, MIDSCALE_MAX_KW,
   MIDSCALE_MIN_KW, MIDSCALE_MAX_KW, START_DATE))

# --- 8.3 (c) EQUITY GAPS: income distribution of host communities ------------
# TTS appends the census-tract median household income for each project's
# location. Comparing tract income to STATE median income tells you whether
# mid-scale solar is landing in above- or below-median communities.
#
# -----------------------------------------------------------------------------
# TWO THINGS TO KNOW BEFORE YOU USE THESE FIELDS
# -----------------------------------------------------------------------------
# 1. THE VALUES ARE BINNED, NOT EXACT.
#    Per the user guide, all six income fields are in thousands of 2025 dollars,
#    binned to $10k, and report the bin's LOWER BOUND -- 60 means "$60k-$70k".
#    Everything is capped at 200, meaning "$200k+".
#
#    Consequences: never print these as precise dollars; the quartile cut points
#    below are coarse; and two tracts showing the same value may differ by up to
#    $10k. Ratios are still meaningful, just blunt.
#
# 2. THERE ARE TWO INCOME SERIES, AND THEY MEAN DIFFERENT THINGS.
#    med_inc_HH_*            median across ALL households -- renters and owners
#    med_inc_owner_occ_HH_*  median across OWNER-OCCUPIED households only
#
#    Renters generally earn less, so removing them raises the median. In this
#    file the owner-occupied figure is higher for 69.9% of records and equal for
#    28.1% (that near-tie is mostly the $10k binning collapsing a real gap).
#    The average difference is +$16.4k overall, +$20.1k among mid-scale tracts.
#
#    WHICH TO USE: for RESIDENTIAL adoption equity, owner-occupied is the fairer
#    benchmark, because rooftop solar effectively requires owning the property --
#    comparing adopters against all households includes renters who were never
#    candidates, overstating how affluent adopters look. DC is the sharp case:
#    $102k across all households vs $165.6k among owner-occupiers.
#
#    For MID-SCALE, the host is usually a business, school or government rather
#    than a household, so neither field describes the customer. They describe the
#    surrounding tract -- useful for "is this project sited in a lower-income
#    community" (the IRA low-income bonus framing), not "is the buyer wealthy".
#    For that community-context question the ALL-household series is the more
#    natural choice, which is what the query below uses.
#
# ntile(4) splits rows into 4 equal-sized buckets (quartiles) ranked by the
# ordering you give it -- here, tract income relative to state income. Bucket 1
# is the lowest-income quartile of host tracts.

run("24_income_distribution", "RQ5(c) -- HOST-TRACT INCOME QUARTILES", sprintf("
  WITH scored AS (
    SELECT
      PV_system_size_DC,
      total_installed_price,
      customer_segment,
      med_inc_HH_tract / NULLIF(med_inc_HH_state, 0)          AS tract_vs_state,
      ntile(4) OVER (ORDER BY med_inc_HH_tract / NULLIF(med_inc_HH_state, 0)) AS income_quartile
    FROM tts
    WHERE %s
      AND med_inc_HH_tract > 0
      AND med_inc_HH_state > 0
  )
  SELECT
    income_quartile,
    count(*)                                                 AS systems,
    round(min(tract_vs_state), 2)                            AS min_tract_to_state_ratio,
    round(max(tract_vs_state), 2)                            AS max_tract_to_state_ratio,
    round(sum(PV_system_size_DC) / 1000.0, 1)                AS mw_dc,
    round(sum(total_installed_price) FILTER (WHERE total_installed_price > 0)
          / 1e6, 1)                                          AS reported_capital_musd
  FROM scored
  GROUP BY income_quartile
  ORDER BY income_quartile
", midscale_filter))

# --- 8.4 Underserved segments: high-need, low-activity --------------------
# Schools, government and tax-exempt hosts are policy priorities (and, since
# the IRA's direct-pay provisions, newly financeable). Are they showing up?

run("25_tax_exempt_hosts", "RQ5 -- TAX-EXEMPT / PUBLIC HOSTS BY YEAR", sprintf("
  SELECT
    year(installation_date)                                  AS install_year,
    count(*) FILTER (WHERE customer_segment = 'SCHOOL')       AS schools,
    count(*) FILTER (WHERE customer_segment = 'GOV')          AS government,
    count(*) FILTER (WHERE customer_segment = 'OTHER TAX-EXEMPT') AS other_tax_exempt,
    count(*) FILTER (WHERE customer_segment = 'AGRICULTURAL') AS agricultural,
    count(*) FILTER (WHERE customer_segment IN ('RES_MF'))    AS multifamily,
    count(*)                                                 AS all_midscale
  FROM tts
  WHERE %s
  GROUP BY install_year
  ORDER BY install_year
", midscale_filter))


# =============================================================================
# SECTION 9 -- PULLING RAW ROWS INTO R
# =============================================================================
#
# Everything above returns small summary tables. When you want the underlying
# project-level rows -- to plot, model, or hand to dplyr -- SELECT only the
# columns you need. Pulling all 94 columns for 4 million rows will exhaust
# your memory; the whole point of DuckDB is to avoid that.

midscale_systems <- q(paste0(dedup_cte(midscale_filter), "
  SELECT
    installation_date,
    state,
    city,
    zip_code,                                  -- raw, as shipped: see query 06h
    -- Normalised 5-digit zip. USE THIS for any join to census, ACS or geography
    -- files -- joining on raw zip_code silently drops California (float
    -- artifact) and most of New England (leading zero lost upstream).
    ", ZIP5_SQL, " AS zip5,
    utility_service_territory,
    customer_segment,
    PV_system_size_DC                          AS kw_dc,
    NULLIF(total_installed_price, -1)          AS installed_price_usd,
    NULLIF(rebate_or_grant, -1)                AS rebate_usd,
    third_party_owned,
    technology_type,
    NULLIF(battery_rated_capacity_kWh, -1)     AS battery_kwh,
    installer_name,
    -- UNITS: thousands of 2025 dollars, BINNED to 10k and reported as the
    -- bin lower bound (60 means 60k-70k), capped at 200 meaning 200k-plus.
    -- The _k suffix is a reminder not to read these as exact dollar figures.
    NULLIF(med_inc_HH_tract, -1)               AS tract_med_inc_k,
    -- Owner-occupied households only (excludes renters), same units/binning.
    -- The better benchmark for residential adoption, since rooftop solar
    -- effectively requires owning the property -- see query 24's notes.
    NULLIF(med_inc_owner_occ_HH_tract, -1)     AS tract_med_inc_owner_occ_k,

    -- Derived flag from query 06e: a 'RES'-labeled system of this size is not
    -- a residential rooftop, it is a community solar garden coded by
    -- subscriber class. Kept as a COLUMN rather than filtered out here, so you
    -- make the include/exclude call explicitly in your own analysis:
    --   subset(midscale_systems, !likely_community_solar)
    -- LIKELY_COMMUNITY_SOLAR_SQL is defined in 00_csv_to_parquet.R (3b).
    ", LIKELY_COMMUNITY_SOLAR_SQL, "               AS likely_community_solar,

    -- TRUE for the first system of each project -- a project being systems at
    -- one address installed within DEDUP_WINDOW_MONTHS of each other.
    -- Filter on it to COUNT projects; ignore it when summing price or capacity,
    -- because each system carries its own phase's cost:
    --   nrow(subset(midscale_systems, is_first_at_address))   <- projects
    --   sum(midscale_systems$installed_price_usd, na.rm=TRUE) <- capital
    TTS_link_ID                                AS link_id,
    first_at_address::DATE                     AS first_install_at_address,
    is_first_at_address
  FROM flagged"))

cat("\nOf those,",
    sum(midscale_systems$likely_community_solar), "are flagged",
    "`likely_community_solar` (see query 06e) --",
    "decide whether to keep them before analyzing by segment or size bucket.\n")

cat("They cover", sum(midscale_systems$is_first_at_address), "distinct addresses;",
    sum(!midscale_systems$is_first_at_address), "rows are later phases or",
    "expansions at an address already counted (see query 06f).\n",
    "Use `is_first_at_address` to count projects -- but NOT when summing",
    "price or capacity, since each row carries its own phase's cost.\n")

cat("\n\nPulled", nrow(midscale_systems), "mid-scale project rows into the",
    "data.frame `midscale_systems` for further analysis in R.\n")

write.csv(midscale_systems,
          file.path(output_dir, "midscale_systems_2018plus.csv"),
          row.names = FALSE)


# =============================================================================
# SECTION 10 -- CLOSE THE CONNECTION
# =============================================================================
# Always disconnect. shutdown = TRUE stops the DuckDB engine and releases the
# file handle on the parquet (otherwise Windows may keep the file locked).

dbDisconnect(con, shutdown = TRUE)

cat("\nDone. All result tables written to:\n  ", output_dir, "\n")


# =============================================================================
# WHAT TO TAKE AWAY -- READ THIS AFTER THE FIRST RUN
# =============================================================================
#
# WHAT TTS CAN ANSWER WELL
#   * How much mid-scale capacity was installed, where, and when (2018-2024)
#   * Reported installed cost and $/W trends, for the ~2/3 of projects with
#     price data
#   * Ownership split: third-party-owned vs. host-owned
#   * Customer segment mix and storage attachment
#   * Which developers are active in which states
#   * Host-community income profile, via census-tract income
#
# WHAT TTS CANNOT ANSWER -- PLAN FOR OTHER SOURCES
#   * WHO INVESTED. There is no capital-provider, lender, or sponsor field.
#     RQ3 cannot be answered from this file. `installer_name` is a developer
#     proxy only.
#   * FINANCING STRUCTURE beyond TPO/host-owned. No tax equity, C-PACE, project
#     debt, or fund-level detail. loan/cash flags are inferred and are weak
#     signal at mid-scale sizes.
#   * NATIONAL TOTALS. Only 27 states appear in this file at all. Anything you
#     compute is a 27-state figure, not a US figure. Say so explicitly.
#   * COMMUNITY SOLAR as a distinct category -- not flagged, and worse,
#     ACTIVELY MISLABELED. See query 06e. Community solar gardens are filed
#     under customer_segment = 'RES' in several states because the record is
#     coded by subscriber class, not by host. Minnesota alone is ~44% of all
#     'RES' mid-scale records and ~33% of the national 1-2 MW size band, at a
#     median of 1,333 kW. Decide explicitly whether to keep, drop, or separate
#     these before reporting anything by segment or by size band.
#   * ANYTHING AFTER THE REPORTING LAG. Check query 05 before charting trends;
#     the last year or two is always incomplete.
#
# SUGGESTED NEXT STEPS
#   1. Run this script and read outputs 03, 04, 05 and 22 first. Together they
#      tell you which of your research questions this dataset can actually
#      support and at what geographic scope.
#   2. Fix your mid-scale definition (Section 1) and re-run with a couple of
#      alternative bands to check that your conclusions aren't an artifact of
#      where you drew the line.
#   3. For RQ3 (investors), scope an external join -- the developer names from
#      query 16 are your join key into ownership and capital-stack data.
# =============================================================================
