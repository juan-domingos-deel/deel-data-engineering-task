"""Run Soda Core scans on a schedule and expose the results as Prometheus
metrics. Closes ADR-0005 § Sub-decision D.

Soda Core's Python API (`soda.scan.Scan`) returns a result list with
`.passed` and `.outcome` attrs per check; we count passed vs failed and
re-emit as Prom counters. /metrics is served on :8002.
"""

import logging
import time

from apscheduler.schedulers.blocking import BlockingScheduler
from prometheus_client import Counter, Gauge, start_http_server
from soda.scan import Scan

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("soda-runner")

soda_check_passed_total = Counter(
    "soda_check_passed_total",
    "Soda checks that passed (cumulative).",
    ["check"],
)
soda_check_failed_total = Counter(
    "soda_check_failed_total",
    "Soda checks that failed (cumulative).",
    ["check"],
)
soda_last_scan_at = Gauge(
    "soda_last_scan_unixtime",
    "Unix timestamp of the last completed Soda scan.",
)


def run_scan() -> None:
    scan = Scan()
    scan.set_data_source_name("analytics")
    scan.add_configuration_yaml_file(file_path="/etc/soda/configuration.yml")
    scan.add_sodacl_yaml_file("/etc/soda/checks.yml")
    scan.set_verbose(False)
    try:
        scan.execute()
    except Exception as exc:
        log.error("scan failed to execute: %s", exc)
        return
    soda_last_scan_at.set(time.time())
    for check in scan.get_checks_text() if hasattr(scan, "get_checks_text") else []:
        # fall-through; we'll use the structured result below.
        pass
    for result in scan.get_scan_results().get("checks", []):
        check_name = result.get("name") or result.get("identity") or "unknown"
        outcome = result.get("outcome", "fail")
        if outcome == "pass":
            soda_check_passed_total.labels(check=check_name).inc()
        else:
            soda_check_failed_total.labels(check=check_name).inc()
            log.warning("soda check failed: %s outcome=%s", check_name, outcome)
    log.info("scan complete: %d checks", len(scan.get_scan_results().get("checks", [])))


def main() -> None:
    log.info("starting soda runner; metrics on :8002")
    start_http_server(8002)
    sched = BlockingScheduler(timezone="UTC")
    # Run once immediately, then every 60 s.
    run_scan()
    sched.add_job(run_scan, "interval", seconds=60, id="critical-scan")
    sched.start()


if __name__ == "__main__":
    main()
