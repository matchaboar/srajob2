#!/usr/bin/env python3
"""
Real-time monitoring script for job scraping throughput.

Usage:
    # Monitor with default 5-minute window
    uv run python scripts/monitor_throughput.py

    # Monitor with custom window (300 seconds = 5 minutes)
    uv run python scripts/monitor_throughput.py --window 300

    # Continuous monitoring (refreshes every 10 seconds)
    watch -n 10 'uv run python scripts/monitor_throughput.py --window 60'
"""
import argparse
import asyncio
import json
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from job_scrape_application.workflows.activities import record_throughput_metrics
from job_scrape_application.dbos_runtime import queue as dbos_queue


async def main():
    parser = argparse.ArgumentParser(
        description="Monitor job scraping throughput and queue status"
    )
    parser.add_argument(
        "--window",
        type=int,
        default=300,
        help="Time window in seconds to calculate throughput (default: 300 = 5 minutes)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON instead of formatted text",
    )
    parser.add_argument(
        "--queue-status",
        action="store_true",
        help="Also show detailed queue status",
    )
    args = parser.parse_args()

    # Get throughput metrics
    try:
        metrics = await record_throughput_metrics(window_seconds=args.window)
    except Exception as e:
        print(f"Error: Failed to get throughput metrics: {e}", file=sys.stderr)
        sys.exit(1)

    if args.json:
        # JSON output for programmatic use
        result = {"metrics": metrics}
        if args.queue_status:
            result["queue_status"] = dbos_queue.queue_status()
        print(json.dumps(result, indent=2))
    else:
        # Human-readable output
        print("=" * 60)
        print("Job Scraping Throughput Monitor")
        print("=" * 60)
        print()
        print(f"Time Window: {args.window} seconds ({args.window/60:.1f} minutes)")
        print()
        print("THROUGHPUT METRICS:")
        print(f"  ✓ URLs/minute:     {metrics['throughputPerMinute']:.2f}")
        print(f"  ✓ Completed:       {metrics['completedInWindow']} URLs")
        print(f"  ✗ Failed:          {metrics['failedInWindow']} URLs")
        if metrics['completedInWindow'] > 0:
            fail_rate = (metrics['failedInWindow'] / (metrics['completedInWindow'] + metrics['failedInWindow'])) * 100
            print(f"  ✗ Failure rate:    {fail_rate:.1f}%")
        print()
        print("CURRENT QUEUE STATUS:")
        print(f"  ⏳ Pending:         {metrics['pending']} URLs")
        print(f"  ⚙️  Processing:      {metrics['currentlyProcessing']} URLs")
        print()

        # Show performance indicators
        throughput = metrics['throughputPerMinute']
        if throughput >= 100:
            status = "✅ TARGET MET"
            color = "\033[92m"  # Green
        elif throughput >= 80:
            status = "⚠️  CLOSE TO TARGET"
            color = "\033[93m"  # Yellow
        elif throughput >= 50:
            status = "⚠️  BELOW TARGET"
            color = "\033[93m"  # Yellow
        else:
            status = "❌ WELL BELOW TARGET"
            color = "\033[91m"  # Red
        reset = "\033[0m"

        print(f"{color}STATUS: {status} (Target: 100 URLs/min){reset}")
        print()

        if args.queue_status:
            print("=" * 60)
            print("DETAILED QUEUE STATUS:")
            print("=" * 60)
            status = dbos_queue.queue_status()
            for queue_name, queue_data in status.items():
                print(f"\n{queue_name.upper()} QUEUE:")
                for status_name, count in queue_data.items():
                    print(f"  {status_name}: {count}")
            print()

        # Show recommendations
        if throughput < 100:
            print("RECOMMENDATIONS:")
            if metrics['pending'] == 0 and metrics['currentlyProcessing'] == 0:
                print("  • No URLs in queue - throughput limited by work availability")
            elif metrics['currentlyProcessing'] < 20:
                print("  • Low concurrent processing - consider increasing worker count or concurrency")
            elif metrics['failedInWindow'] > metrics['completedInWindow'] * 0.1:
                print("  • High failure rate - investigate errors before scaling")
            else:
                print("  • Consider implementing Phase 1-3 optimizations from the plan")
            print()


if __name__ == "__main__":
    asyncio.run(main())
