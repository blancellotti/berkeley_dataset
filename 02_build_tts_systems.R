# =============================================================================
# STEP 2 -- Tracking the Sun: one row per PV system for the project universe
# =============================================================================
#
# PURPOSE
#   Export the LBNL Distributed Solar Public Data file ("Tracking the Sun") as a
#   clean, one-row-per-system table whose shared columns match the other
#   sources in the IOF project universe (../eia860, ../USPVDB, ../ferc556,
#   ../NLR). This script uses Tracking the Sun ONLY; linking to the other
#   sources happens later, in the merge script.
#
#   01_explore_tts_coverage.R is the ANALYSIS of this file. This script is the
#   EXPORT. Both source 00_csv_to_parquet.R for the paths, the connection and
#   the shared definitions (ZIP5_SQL, LIKELY_COMMUNITY_SOLAR_SQL, dedup_cte()),
#   so they can never disagree about a zip, a project or a community garden.
#
# PIPELINE
#   00_csv_to_parquet.R        data prep, paths, shared definitions
#   01_explore_tts_coverage.R  the coverage analysis
#   02_build_tts_systems.R     <- you are here (export for the universe)
#
# -----------------------------------------------------------------------------
# DECISIONS (agreed with the research team)
# -----------------------------------------------------------------------------
#   1. ROWS: every row with a PV size (PV_system_size_DC > 0), ALL YEARS, with
#      in_window = installed 2018 or later. Storage-only rows are dropped --
#      they are not solar. Rows stay SYSTEMS; nothing is collapsed, because
#      each phase row carries its own capacity and cost (see README).
#   2. FORMAT: parquet only (~4 million rows). Read it in R with
#      arrow::read_parquet() or query it with DuckDB.
#   3. AC CAPACITY: TTS reports DC only. capacity_mw_ac = DC / loading ratio,
#      using the system's own inverter_loading_ratio when it is plausible
#      (0.8-2.0), otherwise the median ratio of plausible values for the same
#      install year and customer segment (then year only). capacity_ac_method
#      says which. TTS's own ratios are used, not EIA's utility-scale ~1.33,
#      because small systems run lower (~1.15-1.25).
#   4. SHARED DEFINITIONS live in 00_csv_to_parquet.R (Section 3b).
#
#   LBNL's -1 missing-value code becomes NULL in every cleaned column. The
#   parquet built by step 0 keeps the raw -1s if you need to trace a value.
#
#   source_record_id = "tts_" + the row's position in the release file
#   (1-based). LBNL's own data_provider + system_ID pair is NOT unique and is
#   missing on ~93k rows, so it is kept but not used as the key. The position
#   is stable for a given release, not across releases.
#
# HOW TO RUN
#   Open berkeley.Rproj in RStudio and Source this file, or from this folder:
#       Rscript 02_build_tts_systems.R
#   Output: output/tts_systems.parquet
#
# =============================================================================


# =============================================================================
# SECTION 0 -- SETUP
# =============================================================================

PIPELINE_SOURCED <- TRUE
source("00_csv_to_parquet.R")     # paths, connect_tts(), shared definitions

con <- connect_tts()
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
invisible(dbExecute(con, "SET memory_limit = '4GB'"))

START_DATE     <- "2018-01-01"
SOURCE_VINTAGE <- DATA_RELEASE        # e.g. TTS_LBNL_public_file_28-Jul-2026_all
ILR_PLAUSIBLE  <- c(0.8, 2.0)         # a system's own ratio is used only in here

OUT_PARQUET <- file.path(OUTPUT_DIR, "tts_systems.parquet")

q <- function(sql) dbGetQuery(con, sql)

cat("--- TTS system export ---\n")
cat("Release :", SOURCE_VINTAGE, "\n")
cat("Window  : all years kept; in_window = installed on or after", START_DATE, "\n")
cat("Output  :", OUT_PARQUET, "\n")
cat("-------------------------\n\n")

# The size ladder, identical to ../eia860, ../USPVDB, ../ferc556, ../NLR and
# 01's SIZE_BIN_SQL. HALF-OPEN [lower, upper), in kW. `%s` is the kW column.
size_band_sql <- function(kw) {
  sprintf("
      CASE
        WHEN %1$s IS NULL  THEN NULL
        WHEN %1$s <     5 THEN '01. under 5 kW'
        WHEN %1$s <    10 THEN '02. 5-10 kW'
        WHEN %1$s <    20 THEN '03. 10-20 kW'
        WHEN %1$s <    50 THEN '04. 20-50 kW'
        WHEN %1$s <   100 THEN '05. 50-100 kW'
        WHEN %1$s <   250 THEN '06. 100-250 kW'
        WHEN %1$s <   500 THEN '07. 250-500 kW'
        WHEN %1$s <  1000 THEN '08. 500 kW-1 MW'
        WHEN %1$s <  2000 THEN '09. 1-2 MW'
        WHEN %1$s <  5000 THEN '10. 2-5 MW'
        ELSE                   '11. 5 MW and above'
      END", kw)
}

# LBNL flags are 1 / 0 / -1 (missing) stored as numbers; make them booleans.
flag_sql <- function(col) {
  sprintf("CASE WHEN %1$s = 1 THEN TRUE WHEN %1$s = 0 THEN FALSE END", col)
}


# =============================================================================
# SECTION 1 -- THE ROWS: PV SYSTEMS, WITH THEIR FILE POSITION
# =============================================================================
# read_parquet(..., file_row_number = true) exposes each row's position in the
# file; +1 makes it 1-based. That position is the record ID.

invisible(dbExecute(con, sprintf("
  CREATE OR REPLACE VIEW tts_pv AS
  SELECT *, file_row_number + 1 AS tts_row
  FROM read_parquet('%s', file_row_number = true)
  WHERE PV_system_size_DC > 0
    AND technology_type <> 'storage-only'
", PARQUET_PATH)))

n_all     <- q("SELECT count(*) AS n FROM tts")$n
n_pv      <- q("SELECT count(*) AS n FROM tts_pv")$n
n_storage <- q("SELECT count(*) AS n FROM tts WHERE technology_type = 'storage-only'")$n
cat(sprintf("Rows in release: %s | PV rows kept: %s | storage-only dropped: %s | no PV size dropped: %s\n\n",
            format(n_all, big.mark = ","), format(n_pv, big.mark = ","),
            format(n_storage, big.mark = ","),
            format(n_all - n_pv - n_storage, big.mark = ",")))


# =============================================================================
# SECTION 2 -- MEDIAN LOADING RATIOS (for systems without a plausible one)
# =============================================================================
# Medians of plausible ratios by install year x customer segment, and by year
# alone as the next fallback. Cells with fewer than 30 systems are not used.

invisible(dbExecute(con, sprintf("
  CREATE OR REPLACE TABLE ilr_year_seg AS
  SELECT year(installation_date) AS yr, customer_segment,
         median(inverter_loading_ratio) AS ilr, count(*) AS n
  FROM tts_pv
  WHERE inverter_loading_ratio BETWEEN %1$f AND %2$f
  GROUP BY 1, 2 HAVING count(*) >= 30;

  CREATE OR REPLACE TABLE ilr_year AS
  SELECT year(installation_date) AS yr,
         median(inverter_loading_ratio) AS ilr, count(*) AS n
  FROM tts_pv
  WHERE inverter_loading_ratio BETWEEN %1$f AND %2$f
  GROUP BY 1 HAVING count(*) >= 30;
", ILR_PLAUSIBLE[1], ILR_PLAUSIBLE[2])))

ilr_overall <- q(sprintf(
  "SELECT median(inverter_loading_ratio) AS ilr FROM tts_pv
   WHERE inverter_loading_ratio BETWEEN %f AND %f",
  ILR_PLAUSIBLE[1], ILR_PLAUSIBLE[2]))$ilr


# =============================================================================
# SECTION 3 -- BUILD AND WRITE THE SYSTEM TABLE
# =============================================================================
# dedup_cte() (from 00) adds is_first_at_address and first_at_address, using
# the same 12-month address rule as 01. It is applied to the PV rows only.

select_sql <- sprintf("
  %s,
  priced AS (
    SELECT f.*,
      -- exact duplicates: same address link, size AND price to the cent
      CASE
        WHEN TTS_link_ID IN ('-1', 'redacted') OR total_installed_price <= 0
          THEN FALSE
        ELSE count(*) OVER (PARTITION BY TTS_link_ID, PV_system_size_DC,
                            total_installed_price) > 1
      END AS exact_duplicate,
      coalesce(ys.ilr, y.ilr, %f) AS ilr_median
    FROM flagged f
    LEFT JOIN ilr_year_seg ys ON ys.yr = year(f.installation_date)
                             AND ys.customer_segment = f.customer_segment
    LEFT JOIN ilr_year y      ON y.yr  = year(f.installation_date)
  )
  SELECT
    -- IDs and provenance ------------------------------------------------------
    'tts_' || tts_row                               AS source_record_id,
    tts_row,
    NULLIF(data_provider_1, '-1')                   AS data_provider_1,
    NULLIF(system_ID_1, '-1')                       AS system_id_1,
    NULLIF(data_provider_2, '-1')                   AS data_provider_2,
    NULLIF(system_ID_2, '-1')                       AS system_id_2,
    NULLIF(NULLIF(TTS_link_ID, '-1'), 'redacted')   AS link_id,
    NULLIF(extensions_multiphase_id, '-1')          AS multiphase_id,
    '%s'                                            AS source_vintage,
    installation_date >= DATE '%s'                  AS in_window,

    -- Grouping (see 00, Section 3b) --------------------------------------------
    is_first_at_address,
    first_at_address::DATE                          AS first_install_at_address,
    exact_duplicate,
    NULLIF(expansion_system, '-1')                  AS expansion_system,
    NULLIF(multiple_phase_system, -1)               AS multiple_phase_system,

    -- Date ---------------------------------------------------------------------
    installation_date::DATE                         AS build_date,
    year(installation_date)                         AS build_year,
    'installed'                                     AS status_group,

    -- Capacity: DC reported, AC converted ---------------------------------------
    PV_system_size_DC / 1000                        AS capacity_mw_dc,
    NULLIF(inverter_loading_ratio, -1)              AS inverter_loading_ratio_raw,
    CASE WHEN inverter_loading_ratio BETWEEN %f AND %f
         THEN inverter_loading_ratio ELSE ilr_median END
                                                    AS dc_ac_ratio_used,
    PV_system_size_DC / 1000 /
      CASE WHEN inverter_loading_ratio BETWEEN %f AND %f
           THEN inverter_loading_ratio ELSE ilr_median END
                                                    AS capacity_mw_ac,
    CASE WHEN inverter_loading_ratio BETWEEN %f AND %f
         THEN 'system loading ratio'
         ELSE 'median loading ratio (year x segment)' END
                                                    AS capacity_ac_method,
    %s                                              AS size_band_dc,
    %s                                              AS size_band_ac,

    -- Location -------------------------------------------------------------------
    NULLIF(zip_code, '-1')                          AS zip_code,
    %s                                              AS zip5,
    NULLIF(NULLIF(city, '-1'), 'redacted')          AS city,
    state,
    NULLIF(utility_service_territory, '-1')         AS utility_service_territory,

    -- Segment ----------------------------------------------------------------------
    NULLIF(customer_segment, '-1')                  AS customer_segment,
    %s                                              AS likely_community_solar,

    -- Cost (nominal USD, as reported, gross of incentives) ---------------------------
    NULLIF(total_installed_price, -1)               AS installed_price_usd,
    CASE WHEN total_installed_price > 0
         THEN total_installed_price / (PV_system_size_DC * 1000) END
                                                    AS price_per_w_dc,
    NULLIF(rebate_or_grant, -1)                     AS rebate_or_grant_usd,
    NULLIF(battery_price, -1)                       AS battery_price_usd,

    -- Ownership and finance ------------------------------------------------------------
    %s                                              AS third_party_owned,
    %s                                              AS third_party_owned_inferred,
    NULLIF(third_party_owned_confidence, '-1')      AS third_party_owned_confidence,
    %s                                              AS loan_inferred,
    NULLIF(loan_inferred_confidence, '-1')          AS loan_inferred_confidence,
    %s                                              AS cash_inferred,
    NULLIF(cash_inferred_confidence, '-1')          AS cash_inferred_confidence,

    -- Installer and design ----------------------------------------------------------------
    NULLIF(installer_name, '-1')                    AS installer_name,
    %s                                              AS self_installed,
    %s                                              AS ground_mounted,
    %s                                              AS tracking,
    %s                                              AS new_construction,
    NULLIF(technology_type, '-1')                   AS technology_type,
    NULLIF(battery_rated_capacity_kW, -1)           AS battery_kw,
    NULLIF(battery_rated_capacity_kWh, -1)          AS battery_kwh,

    -- Neighbourhood income: thousands of 2025 $, binned to $10k, lower bound --------------
    NULLIF(med_inc_HH_tract, -1)                    AS tract_med_inc_k,
    NULLIF(med_inc_owner_occ_HH_tract, -1)          AS tract_med_inc_owner_occ_k
  FROM priced
",
  dedup_cte("TRUE", from_table = "tts_pv"),
  ilr_overall,
  SOURCE_VINTAGE, START_DATE,
  ILR_PLAUSIBLE[1], ILR_PLAUSIBLE[2],
  ILR_PLAUSIBLE[1], ILR_PLAUSIBLE[2],
  ILR_PLAUSIBLE[1], ILR_PLAUSIBLE[2],
  size_band_sql("PV_system_size_DC"),
  size_band_sql(sprintf(
    "(PV_system_size_DC / CASE WHEN inverter_loading_ratio BETWEEN %f AND %f
      THEN inverter_loading_ratio ELSE ilr_median END)",
    ILR_PLAUSIBLE[1], ILR_PLAUSIBLE[2])),
  ZIP5_SQL, LIKELY_COMMUNITY_SOLAR_SQL,
  flag_sql("third_party_owned"), flag_sql("third_party_owned_inferred"),
  flag_sql("loan_inferred"), flag_sql("cash_inferred"),
  flag_sql("self_installed"), flag_sql("ground_mounted"),
  flag_sql("tracking"), flag_sql("new_construction")
)

cat("Writing", OUT_PARQUET, "...\n")
elapsed <- system.time(
  dbExecute(con, sprintf(
    "COPY (%s ORDER BY tts_row) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)",
    select_sql, OUT_PARQUET))
)
cat("Done in", round(elapsed[["elapsed"]], 1), "seconds.\n\n")

invisible(dbExecute(con, sprintf(
  "CREATE OR REPLACE VIEW out AS SELECT * FROM read_parquet('%s')", OUT_PARQUET)))


# =============================================================================
# SECTION 4 -- CHECKS
# =============================================================================

cat("Checks\n")
check <- function(ok, msg) {
  if (!isTRUE(ok)) stop("CHECK FAILED: ", msg)
  cat("  ok  ", msg, "\n")
}

o <- q("
  SELECT count(*) AS n,
         count(DISTINCT source_record_id) AS n_id,
         sum(capacity_mw_dc) AS mw_dc,
         count(*) FILTER (WHERE technology_type = 'storage-only') AS n_storage,
         count(*) FILTER (WHERE capacity_mw_ac IS NULL OR capacity_mw_ac <= 0) AS n_no_ac,
         count(*) FILTER (WHERE in_window) AS n_window,
         count(*) FILTER (WHERE zip_code = 'redacted') AS zip_redacted,
         count(*) FILTER (WHERE zip5 IS NOT NULL AND NOT regexp_matches(zip5, '^[0-9]{5}$')) AS zip_garbage,
         count(*) FILTER (WHERE price_per_w_dc IS NOT NULL) AS n_priced
  FROM out")
src_mw <- q("SELECT sum(PV_system_size_DC) / 1000 AS mw FROM tts_pv")$mw

check(o$n == n_pv, "every PV row is in the output")
check(o$n_id == o$n, "source_record_id is unique")
check(isTRUE(all.equal(o$mw_dc, src_mw)), "DC capacity sums match the release")
check(o$n_storage == 0, "no storage-only rows")
check(o$n_no_ac == 0, "every system has a positive converted AC capacity")

expect <- function(actual, expected, what) {
  if (!identical(as.numeric(actual), as.numeric(expected))) {
    warning(sprintf("%s: got %s, profile said %s", what,
                    format(actual, big.mark = ","),
                    format(expected, big.mark = ",")), call. = FALSE)
  } else {
    cat(sprintf("  ok   %s = %s\n", what, format(actual, big.mark = ",")))
  }
}
# 2,611,474 = PV rows installed 2018+ excluding storage-only. (2,402 storage-
# only rows from 2018+ also carry a PV size -- batteries added to an existing
# array -- and are dropped with the other storage-only rows.)
expect(o$n_window, 2611474, "PV systems installed 2018 or later")
cat(sprintf(paste0("  note  zip5: %s rows have zip_code = 'redacted' (zip5 blank); ",
                   "%s malformed values (phone numbers, city names) not normalised\n"),
            format(o$zip_redacted, big.mark = ","),
            format(o$zip_garbage, big.mark = ",")))


# =============================================================================
# SECTION 5 -- SUMMARY
# =============================================================================

cat("\nMedian loading ratio used when a system has none (by year, all segments)\n")
print(q("SELECT yr AS build_year, round(ilr, 3) AS median_ilr, n
         FROM ilr_year WHERE yr >= 2018 ORDER BY yr"), row.names = FALSE)

cat("\nAC conversion method (all rows)\n")
print(q("SELECT capacity_ac_method, count(*) AS systems FROM out
         GROUP BY 1 ORDER BY 2 DESC"), row.names = FALSE)

cat("\nIn-window systems by DC size band\n")
print(q("SELECT size_band_dc, count(*) AS systems,
                round(sum(capacity_mw_dc)) AS mw_dc,
                round(100.0 * count(price_per_w_dc) / count(*), 1) AS pct_priced
         FROM out WHERE in_window GROUP BY 1 ORDER BY 1"), row.names = FALSE)

cat("\nIn-window flags\n")
print(q("SELECT count(*) FILTER (WHERE likely_community_solar) AS likely_community_solar,
                count(*) FILTER (WHERE exact_duplicate)        AS exact_duplicate,
                count(*) FILTER (WHERE NOT is_first_at_address) AS later_phase_at_address,
                count(DISTINCT state)                          AS states
         FROM out WHERE in_window"), row.names = FALSE)

size_mb <- round(file.info(OUT_PARQUET)$size / 1024^2, 1)
cols <- nrow(q("DESCRIBE out"))
cat(sprintf("\nWrote %s systems x %d columns to %s (%s MB)\n",
            format(o$n, big.mark = ","), cols, OUT_PARQUET, size_mb))
