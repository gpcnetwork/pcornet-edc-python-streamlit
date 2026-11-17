DQ_RESULTS_TABLE  = "DQ_CHECK_RESULTS"
DQ_RUNS_TABLE     = "DQ_RUNS"
DQ_NETWORKS_TABLE = "DQ_NETWORKS"
DQ_SITES_TABLE    = "DQ_SITES"

QUERY_PACKAGE = "DC V7.03"

# Network benchmark provenance shown with the "% DataMarts w/ exceptions" column
# (from the edc_reference dc_summary shipped with the v7.03 package).
NETWORK_BENCHMARK_ASOF = "April 27, 2026"

# Checks whose exceptions render RED (must be corrected); everything else renders
# BLUE (investigative). SAS hardcodes this list (edc_report.sas %print_dcsummary,
# ~11309) — it is NOT the same as Type == "Required" (e.g. 1.14 is excluded).
REQUIRED_RED_CHECKS = {
    "1.01", "1.02", "1.03", "1.04", "1.05", "1.06", "1.07", "1.08", "1.09",
    "1.10", "1.11", "1.12", "1.13", "1.15", "1.17", "1.18", "1.21", "1.22",
    "3.04", "3.05",
}


def fmt_ms(ms) -> str:
    ms = int(ms or 0)
    if ms < 1_000:
        return f"{ms}ms"
    if ms < 60_000:
        return f"{ms / 1000:.1f}s"
    m, s = divmod(ms // 1000, 60)
    return f"{m}m {s}s"
