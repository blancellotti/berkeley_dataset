# =============================================================================
# STEP 0 -- Data preparation: convert the TTS CSV to Parquet
# =============================================================================
#
# PURPOSE
#   Turn the 2.3 GB LBNL "Tracking the Sun" CSV into a ~130 MB parquet file
#   that 01_explore_tts_coverage.R can query in seconds.
#
#   This file also owns everything about WHERE THE DATA LIVES -- the release
#   name, the file paths, and the helper that opens a database connection.
#   Script 01 sources this file to get those, so the two can never disagree
#   about which data they are working with.
#
# YOU USUALLY DO NOT NEED TO RUN THIS BY HAND
#   Script 01 sources this file and builds the parquet automatically if it is
#   missing. Run this one directly only to FORCE a rebuild -- after dropping in
#   a new LBNL release, or if you suspect the parquet is corrupt. To rebuild
#   over an existing file, set OVERWRITE <- TRUE below.
#
# PIPELINE
#   00_csv_to_parquet.R        <- you are here (data prep + paths)
#   01_explore_tts_coverage.R  the analysis
#
# -----------------------------------------------------------------------------
# "IS THIS EASIER IN PYTHON?" -- NO. THIS IS THE SHORT ANSWER.
# -----------------------------------------------------------------------------
#   The entire conversion is one SQL statement:
#
#       COPY (SELECT * FROM read_csv(...)) TO 'out.parquet' (FORMAT PARQUET)
#
#   DuckDB is the same engine in R and in Python, so you get identical
#   performance and an identical output file either way. There is no R penalty.
#
#   What you should NOT do is the "obvious" R approach:
#
#       df <- read.csv("...2.3GB...")        # <-- don't
#       arrow::write_parquet(df, "out.parquet")
#
#   That loads all 4 million rows into RAM first and will likely fail or thrash
#   on a laptop. The same trap exists in Python with pandas.read_csv().
#
#   DuckDB avoids it by STREAMING: it reads the CSV in chunks and writes
#   parquet chunks as it goes, so peak memory stays low no matter how big the
#   input is. That streaming behavior -- not the language -- is the thing that
#   makes this work.
#
# =============================================================================


# =============================================================================
# SECTION 0 -- SETUP: WHICH FILE, AND WHERE
# =============================================================================

# install.packages(c("DBI", "duckdb", "here"))

library(DBI)      # generic database interface: dbGetQuery(), dbConnect(), ...
library(duckdb)   # the DuckDB engine itself
library(here)     # resolves paths relative to the project root

# --- Which data release? ----------------------------------------------------
# One release, named explicitly. When LBNL publishes a new file, change this
# ONE string and both scripts follow -- the CSV read and the parquet written
# are both derived from it.
#
# Naming the release rather than auto-detecting it is deliberate: the file name
# is part of your provenance. Anyone reading a result can see exactly which
# vintage produced it, and re-running an old analysis can't silently pick up
# newer data.

DATA_RELEASE <- "TTS_LBNL_public_file_28-Jul-2026_all"

# --- Where things live ------------------------------------------------------
# here() finds the project root by looking for the marker that defines it
# (berkeley.Rproj), starting from wherever you happen to be. So these paths
# resolve correctly whether you open the .Rproj in RStudio, run
# `Rscript 01_explore_tts_coverage.R`, or move the project to another machine.
# Nothing machine-specific is hardcoded.
#
# here() also returns forward slashes on Windows, which is what DuckDB needs --
# inside a SQL string a backslash is an escape character and would break the
# query.

CSV_PATH     <- here(paste0(DATA_RELEASE, ".csv"))
PARQUET_PATH <- here(paste0(DATA_RELEASE, ".parquet"))
OUTPUT_DIR   <- here("output")

if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

if (!file.exists(CSV_PATH) && !file.exists(PARQUET_PATH)) {
  stop("Neither the CSV nor the parquet for release '", DATA_RELEASE,
       "' was found in:\n  ", here(),
       "\nCheck DATA_RELEASE at the top of 00_csv_to_parquet.R.")
}

# Safety catch: writing parquet is destructive if a good file is already there.
# Flip to TRUE only when you actually intend to rebuild.
OVERWRITE <- FALSE

# Print what we resolved, every time either script starts. If a result ever
# looks wrong, this tells you at a glance which release you were reading.
cat("--- TTS pipeline ---\n")
cat("Release :", DATA_RELEASE, "\n")
cat("CSV     :", if (file.exists(CSV_PATH)) "present" else "(not present)", "\n")
cat("Parquet :", if (file.exists(PARQUET_PATH)) "present"
                 else "(not present) <- will be built", "\n")
cat("Output  :", OUTPUT_DIR, "\n")
cat("--------------------\n\n")


# =============================================================================
# SECTION 1 -- THE COLUMNS YOU MUST TYPE BY HAND
# =============================================================================
#
# DuckDB infers each column's data type by SNIFFING a sample of rows. It is
# right the vast majority of the time. But sniffing is a GUESS, and there is a
# specific family of columns where a wrong guess is both silent and
# destructive: IDENTIFIERS THAT HAPPEN TO LOOK LIKE NUMBERS.
#
#   zip_code
#     If DuckDB decides this is a number, "02138" (Cambridge, MA) becomes the
#     integer 2138. Every zip in MA, NH, ME, RI, CT, NJ, VT and DC -- i.e. most
#     of the Northeast, which is most of this dataset's mid-scale activity --
#     loses its leading zero and silently stops joining to census/ACS data.
#     There are 230,161 such zips in this file.
#
#   extensions_multiphase_id, TTS_link_ID, system_ID_1, system_ID_2
#     Project and system identifiers. Left to the sniffer, at least one of
#     these DOES get typed as BIGINT on this file, which (a) diverges from the
#     schema LBNL ships and (b) will mangle any ID that has a leading zero or
#     that overflows a 64-bit integer in a future release.
#
# The rule: an identifier is a LABEL, not a quantity. You never do arithmetic
# on one. Force every one of them to text rather than relying on whether this
# particular release happens to sniff correctly.
#
# `types = ` overrides the sniffer for the named columns only; the other 89
# columns are still inferred automatically.

column_type_overrides <- paste0("{",
  "'zip_code': 'VARCHAR', ",
  "'system_ID_1': 'VARCHAR', ",
  "'system_ID_2': 'VARCHAR', ",
  "'TTS_link_ID': 'VARCHAR', ",
  "'extensions_multiphase_id': 'VARCHAR'",
"}")


# =============================================================================
# SECTION 2 -- THE CONVERSION, AS A FUNCTION
# =============================================================================
#
# Wrapped in a function rather than run at the top level, so that script 01 can
# source this file to get the paths WITHOUT triggering a 70-second rebuild.
# The call that actually runs it is at the bottom.

build_parquet <- function() {

  # `sample_size = -1` tells DuckDB to scan the WHOLE file when inferring the
  # remaining 89 types rather than just the first chunk. Slower, but it means a
  # weird value on row 3,000,000 can't produce a type that fails mid-conversion.
  # Worth it for a one-time job.
  read_csv_call <- sprintf(
    "read_csv('%s', header = true, types = %s, sample_size = -1)",
    CSV_PATH, column_type_overrides
  )

  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
  # ^ on.exit() guarantees the connection closes even if the conversion errors
  #   partway through. Without it, a failure would leave the parquet file
  #   locked by a dangling DuckDB handle on Windows.

  # Optional guardrails. DuckDB streams, so it will not blow up without these,
  # but capping memory keeps your machine responsive during the run.
  dbExecute(con, "SET memory_limit = '4GB'")
  dbExecute(con, "SET preserve_insertion_order = false")  # allows parallelism

  # Reading the SQL:
  #   COPY ( <any query> ) TO '<file>' (options)
  #     writes the result of the query straight to disk.
  #
  #   FORMAT PARQUET   the output format.
  #   COMPRESSION ZSTD better ratio than the default SNAPPY and still fast to
  #                    read. This is why 2.3 GB collapses to ~130 MB.
  #   ROW_GROUP_SIZE   how many rows go in each independently-readable block.
  #                    ~122k is a good default: big enough that compression
  #                    works well, small enough that a filtered query can skip
  #                    most blocks without reading them.
  #
  # SELECT * keeps all 94 columns. You could narrow the list here, but don't --
  # disk is cheap and you'll want a field eventually.

  cat("Converting CSV -> parquet. This takes roughly 1-2 minutes.\n")
  cat("  from:", CSV_PATH, "\n")
  cat("    to:", PARQUET_PATH, "\n\n")

  elapsed <- system.time({
    dbExecute(con, sprintf("
      COPY (SELECT * FROM %s)
      TO '%s'
      (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)
    ", read_csv_call, PARQUET_PATH))
  })

  cat("Done in", round(elapsed[["elapsed"]], 1), "seconds.\n\n")

  # --- Verify ---------------------------------------------------------------
  # NEVER trust a data conversion you haven't checked. Four cheap tests:
  #   1. same row count in both files
  #   2. same column count
  #   3. zip codes still have their leading zeros
  #   4. every identifier column landed as text
  #
  # If any fail, we stop rather than let script 01 analyze a corrupt file.

  cat("--- Verification ---\n")

  n_csv <- dbGetQuery(con, sprintf("SELECT count(*) AS n FROM %s", read_csv_call))$n
  n_pq  <- dbGetQuery(con, sprintf("SELECT count(*) AS n FROM read_parquet('%s')",
                                   PARQUET_PATH))$n

  cat("Rows in CSV     :", format(n_csv, big.mark = ","), "\n")
  cat("Rows in parquet :", format(n_pq,  big.mark = ","), "\n")
  if (n_csv != n_pq) stop("ROW COUNT MISMATCH -- conversion is not trustworthy.")

  schema <- dbGetQuery(con, sprintf("DESCRIBE SELECT * FROM read_parquet('%s')",
                                    PARQUET_PATH))
  cat("Columns         :", nrow(schema), "\n")

  # The leading-zero test. `zip_code LIKE '0%'` finds zips starting with zero;
  # if the column had been silently converted to a number, this returns 0 rows.
  n_leading_zero <- dbGetQuery(con, sprintf("
    SELECT count(*) AS n
    FROM read_parquet('%s')
    WHERE zip_code LIKE '0%%'
  ", PARQUET_PATH))$n
  # ^ '%%' not '%' -- sprintf() treats %% as an escaped percent sign.

  cat("Zips starting with 0 :", format(n_leading_zero, big.mark = ","),
      if (n_leading_zero > 0) " (good -- leading zeros preserved)\n"
      else " (!! PROBLEM -- zip_code may have been coerced to a number)\n")

  id_cols  <- c("zip_code", "system_ID_1", "system_ID_2",
                "TTS_link_ID", "extensions_multiphase_id")
  id_types <- schema$column_type[match(id_cols, schema$column_name)]
  cat("\nIdentifier columns stored as text:\n")
  for (i in seq_along(id_cols)) {
    ok <- identical(id_types[i], "VARCHAR")
    cat("  ", formatC(id_cols[i], width = -26), "-> ", id_types[i],
        if (ok) "  ok\n" else "  !! EXPECTED VARCHAR\n", sep = "")
  }
  if (!all(id_types == "VARCHAR")) {
    stop("An identifier column was typed as a number. Fix column_type_overrides.")
  }

  size_csv_mb <- round(file.info(CSV_PATH)$size     / 1024^2, 1)
  size_pq_mb  <- round(file.info(PARQUET_PATH)$size / 1024^2, 1)
  cat("\nCSV size     :", size_csv_mb, "MB\n")
  cat("Parquet size :", size_pq_mb,  "MB  (",
      round(size_csv_mb / size_pq_mb, 1), "x smaller )\n\n")

  invisible(PARQUET_PATH)
}


# =============================================================================
# SECTION 3 -- HELPERS SCRIPT 01 USES
# =============================================================================

# Build the parquet only if it is missing. This is the seam between the two
# scripts: someone who has only the CSV can run 01 and the conversion happens
# automatically, in the right order, without them knowing this file exists.
ensure_parquet <- function() {
  if (file.exists(PARQUET_PATH)) return(invisible(PARQUET_PATH))
  if (!file.exists(CSV_PATH)) {
    stop("No parquet file, and no CSV to build one from.\nExpected: ", CSV_PATH)
  }
  message("\n[pipeline] No parquet found. Building it first...\n")
  build_parquet()
  message("[pipeline] Parquet ready. Continuing to the analysis.\n")
  invisible(PARQUET_PATH)
}

# Open a DuckDB connection with the data already registered as `tts`, so every
# query in script 01 can just say FROM tts.
#
# A VIEW is a saved query that behaves like a table -- it does NOT copy or load
# the data, it just gives the file a short name.
connect_tts <- function() {
  ensure_parquet()
  con <- dbConnect(duckdb())
  dbExecute(con, sprintf(
    "CREATE OR REPLACE VIEW tts AS SELECT * FROM read_parquet('%s')",
    PARQUET_PATH
  ))
  con
}


# =============================================================================
# SECTION 3b -- SHARED ANALYTICAL DEFINITIONS (used by 01 and 02)
# =============================================================================
#
# These are SQL snippets and settings, not transformations: nothing here is
# applied during the conversion, which stays a lossless format change (see
# Appendix 1). They live here because two scripts need them --
# 01_explore_tts_coverage.R (the analysis) and 02_build_tts_systems.R (the
# project-universe export) -- and both source this file. Defining them once
# means the two can never disagree about a zip, a project or a community
# solar garden. The reasoning behind each is written up in 01, Section 1, and
# in README.md.

# --- ZIP5_SQL: normalise the malformed zip_code field ------------------------
# Strips the '.0' float artifact (all of California), splits off the +4, and
# zero-pads to five (New England's lost leading zeros). Join on this, never on
# raw zip_code.
ZIP5_SQL <- "
      CASE
        WHEN zip_code IN ('-1', 'redacted')  THEN NULL
        WHEN zip_code LIKE '%.0'             THEN lpad(split_part(zip_code, '.', 1), 5, '0')
        WHEN zip_code LIKE '%-%'             THEN lpad(split_part(zip_code, '-', 1), 5, '0')
        ELSE                                      lpad(zip_code, 5, '0')
      END"
# ^ lpad(x, 5, '0') left-pads to five characters with zeros, so "1001" becomes
#   "01001" and an already-correct "94103" is left untouched.
#   split_part(x, '.', 1) takes everything before the first dot.

# --- LIKELY_COMMUNITY_SOLAR_SQL ----------------------------------------------
# customer_segment describes the CUSTOMER, so a community solar garden is
# coded by its subscribers' class ('RES'). A 'RES' system of 500 kW-DC or more
# is not a residential rooftop. See 01, query 06e.
LIKELY_COMMUNITY_SOLAR_SQL <- "(customer_segment = 'RES' AND PV_system_size_DC >= 500)"

# --- DEDUP_WINDOW_MONTHS and dedup_cte(): systems -> projects ---------------
# Systems at one address (TTS_link_ID) count as one project if installed
# within DEDUP_WINDOW_MONTHS of each other. Affects COUNTS only; each system
# row carries its own capacity and cost, so those are never de-duplicated.
# `from_table` defaults to the `tts` view that connect_tts() registers.
DEDUP_WINDOW_MONTHS <- 12

# --- Building the flag ------------------------------------------------------
# This needs three stacked CTEs rather than one expression, because a window
# function cannot be nested inside another window function's PARTITION BY.
#
#   dated      stamps each row with the earliest install date at its address
#   clustered  numbers the time windows: months-since-first / window size, so
#              systems 0-11 months in are cluster 0, 12-23 months cluster 1...
#   flagged    marks the first system in each (address, cluster) as the project
#
# dedup_cte() returns that prefix; a query then continues "SELECT ... FROM flagged".

dedup_cte <- function(where_clause, window_months = DEDUP_WINDOW_MONTHS,
                      from_table = "tts") {
  sprintf("
  WITH dated AS (
    SELECT *,
           min(installation_date) OVER (PARTITION BY TTS_link_ID) AS first_at_address
    FROM %s
    WHERE %s
  ),
  clustered AS (
    SELECT *,
      CASE
        WHEN TTS_link_ID IN ('-1', 'redacted') THEN 0
        WHEN %d <= 0 THEN NULL          -- window 0: never collapse
        ELSE floor(date_diff('month', first_at_address, installation_date) / %d)
      END AS window_index
    FROM dated
  ),
  flagged AS (
    SELECT *,
      CASE
        -- no link ID, or window 0: nothing to collapse into, so always primary
        WHEN TTS_link_ID IN ('-1', 'redacted') OR window_index IS NULL THEN TRUE
        -- data_provider_1, system_ID_1 break ties between rows with the same
        -- date and size, so the SAME row is marked first on every run
        -- (without them DuckDB picks arbitrarily; counts are unaffected).
        ELSE row_number() OVER (PARTITION BY TTS_link_ID, window_index
                                ORDER BY installation_date, PV_system_size_DC,
                                         data_provider_1, system_ID_1) = 1
      END AS is_first_at_address
    FROM clustered
  )", from_table, where_clause, window_months, max(window_months, 1))
}
# ^ Two guards worth noting:
#   * Rows with no link ID are forced TRUE before the window function is
#     consulted -- otherwise PARTITION BY would lump every '-1' row into one
#     giant partition and keep a single row out of all of them.
#   * max(window_months, 1) keeps the division safe when the window is 0; the
#     NULL window_index has already short-circuited that case anyway.


# =============================================================================
# SECTION 4 -- RUN THE CONVERSION (only when this script is run DIRECTLY)
# =============================================================================
#
# Script 01 sets PIPELINE_SOURCED <- TRUE before sourcing this file, because it
# only wants the paths and helpers above -- it decides for itself whether a
# rebuild is needed, via ensure_parquet().
#
# When you run THIS file yourself, that flag doesn't exist, so we convert now.

if (!exists("PIPELINE_SOURCED")) {

  if (file.exists(PARQUET_PATH) && !OVERWRITE) {
    stop("Parquet already exists:\n  ", PARQUET_PATH,
         "\nSet OVERWRITE <- TRUE near the top of this script to rebuild it.")
  }

  build_parquet()
  cat("Next step: run 01_explore_tts_coverage.R\n")
}


# =============================================================================
# APPENDIX 1 -- A NOTE ON THE -1 MISSING-VALUE CODE
# =============================================================================
#
# You may be tempted to clean the -1 missing-value codes here, converting them
# to proper NULLs during the conversion. Resist that.
#
# Keep step 0 a faithful, lossless format change: the parquet should contain
# exactly what LBNL shipped, so you can always trace a number back to source.
# Handle -1 in the ANALYSIS layer instead -- that's what the NULLIF(col, -1)
# calls throughout 01_explore_tts_coverage.R are doing.
#
# The general principle: conversion scripts change FORMAT, analysis scripts
# change MEANING. Mixing the two makes it impossible to tell whether a
# surprising result came from the data or from your own cleaning.
#
# -----------------------------------------------------------------------------
# APPENDIX 2 -- ONE HARMLESS SCHEMA DIFFERENCE, IF YOU GO LOOKING
# -----------------------------------------------------------------------------
# If you DESCRIBE this parquet and compare it to the one LBNL distributes, you
# will see installation_date typed as TIMESTAMP here vs TIMESTAMP_NS there.
#
# That is only a storage-precision label -- microseconds vs nanoseconds. The
# date values are the same, and both behave identically in every comparison the
# analysis script makes (e.g. >= DATE '2018-01-01'). Verified: row counts,
# capacity sums, price sums, distinct states, distinct zips and the mid-scale
# project count all match exactly between the two files. No action needed.
# =============================================================================
