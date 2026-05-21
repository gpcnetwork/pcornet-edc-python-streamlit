DQ_RESULTS_TABLE  = "DQ_CHECK_RESULTS"
DQ_RUNS_TABLE     = "DQ_RUNS"
DQ_NETWORKS_TABLE = "DQ_NETWORKS"
DQ_SITES_TABLE    = "DQ_SITES"


def fmt_ms(ms) -> str:
    ms = int(ms or 0)
    if ms < 1_000:
        return f"{ms}ms"
    if ms < 60_000:
        return f"{ms / 1000:.1f}s"
    m, s = divmod(ms // 1000, 60)
    return f"{m}m {s}s"
