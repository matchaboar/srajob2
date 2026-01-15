#!/usr/bin/env python3
"""DBOS Worker TUI - Full-screen dashboard for monitoring queue status and logs."""

import argparse
import os
import re
import signal
import sqlite3
import subprocess
import sys
import threading
from collections import deque
from pathlib import Path

from rich.markup import escape
from textual.app import App, ComposeResult
from textual.containers import Container, Vertical, VerticalScroll
from textual.reactive import reactive
from textual.widgets import DataTable, Footer, Header, Static, TabbedContent, TabPane

# Regex to match ANSI escape codes (SGR and other sequences)
ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]|\x1b\][^\x07]*\x07")

# ANSI SGR code to Rich style mapping
ANSI_FG_COLORS = {
    "30": "black",
    "31": "red",
    "32": "green",
    "33": "yellow",
    "34": "blue",
    "35": "magenta",
    "36": "cyan",
    "37": "white",
    # Bright colors
    "90": "bright_black",
    "91": "bright_red",
    "92": "bright_green",
    "93": "bright_yellow",
    "94": "bright_blue",
    "95": "bright_magenta",
    "96": "bright_cyan",
    "97": "bright_white",
}

ANSI_STYLES = {
    "1": "bold",
    "2": "dim",
    "3": "italic",
    "4": "underline",
}


def ansi_to_rich(text: str) -> str:
    """Convert ANSI escape codes to Rich markup tags.

    Handles SGR (Select Graphic Rendition) sequences for colors and styles.
    Non-SGR sequences are stripped.
    """
    # Pattern to match SGR sequences: ESC [ params m
    sgr_pattern = re.compile(r"\x1b\[([0-9;]*)m")

    result = []
    open_tags: list[str] = []  # Stack of open Rich tags
    last_end = 0

    for match in sgr_pattern.finditer(text):
        # Add text before this match, escaping Rich markup characters
        before_text = text[last_end : match.start()]
        result.append(escape(before_text))
        last_end = match.end()

        params = match.group(1)
        if not params or params == "0":
            # Reset: close all open tags
            for tag in reversed(open_tags):
                result.append(f"[/{tag}]")
            open_tags.clear()
        else:
            # Parse parameters (can be semicolon-separated)
            codes = params.split(";")
            styles_to_apply = []

            for code in codes:
                if code in ANSI_FG_COLORS:
                    styles_to_apply.append(ANSI_FG_COLORS[code])
                elif code in ANSI_STYLES:
                    styles_to_apply.append(ANSI_STYLES[code])
                # Skip unrecognized codes (background colors, etc.)

            if styles_to_apply:
                # Combine styles into a single Rich tag
                combined_style = " ".join(styles_to_apply)
                result.append(f"[{combined_style}]")
                open_tags.append(combined_style)

    # Add remaining text after last match
    remaining_text = text[last_end:]
    # Strip any non-SGR ANSI sequences from remaining text
    remaining_text = ANSI_ESCAPE_RE.sub("", remaining_text)
    result.append(escape(remaining_text))

    # Close any remaining open tags
    for tag in reversed(open_tags):
        result.append(f"[/{tag}]")

    return "".join(result)

# Configuration
MAX_LOG_LINES = 500
REFRESH_INTERVAL = 1.0


class ReverseLogWidget(Static):
    """A log widget that displays newest entries at the top with batched rendering."""

    DEFAULT_CSS = """
    ReverseLogWidget {
        height: auto;
        padding: 0 1;
    }
    """

    RENDER_INTERVAL = 0.2  # Batch render every 200ms

    def __init__(self, max_lines: int = MAX_LOG_LINES, **kwargs):
        super().__init__(markup=True, **kwargs)
        self._lines: deque[str] = deque(maxlen=max_lines)
        self._dirty = False
        self._render_timer = None

    def on_mount(self) -> None:
        """Start the render timer when mounted."""
        self._render_timer = self.set_interval(self.RENDER_INTERVAL, self._maybe_render)

    def write_line(self, text: str) -> None:
        """Add a line to the log (will appear at top). O(1) operation."""
        self._lines.appendleft(text)
        self._dirty = True

    def _maybe_render(self) -> None:
        """Render only if there are pending changes."""
        if not self._dirty:
            return
        self._dirty = False
        self._render_lines()

    def _render_lines(self) -> None:
        """Render all lines with newest at top."""
        if not self._lines:
            self.update("[dim]No output yet...[/dim]")
            return

        self.update("\n".join(self._lines))

    def clear(self) -> None:
        """Clear all log lines."""
        self._lines.clear()
        self._dirty = True


def get_db_connection(db_path: Path) -> sqlite3.Connection | None:
    """Get a database connection if the file exists."""
    if not db_path.exists():
        return None
    try:
        conn = sqlite3.connect(str(db_path), timeout=1.0)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error:
        return None


class QueueSummaryWidget(Static):
    """Widget displaying queue status counts by queue type."""

    def update_data(self, rows: list) -> None:
        """Update with pre-fetched data."""
        if not rows:
            self.update("[dim]Queue empty[/dim]")
            return

        # Organize data by queue type
        queues: dict[str, dict[str, int]] = {}
        for row in rows:
            queue_name, status = row["queue_name"], row["status"]
            if queue_name not in queues:
                queues[queue_name] = {}
            queues[queue_name][status] = queues[queue_name].get(status, 0) + 1

        lines = []
        status_order = ["pending", "processing", "completed", "failed", "skipped"]

        for queue_name in ["listing", "detail"]:
            if queue_name not in queues:
                continue

            queue_data = queues[queue_name]
            queue_style = "cyan" if queue_name == "listing" else "magenta"
            lines.append(f"[bold {queue_style}]━━ {queue_name.upper()} ━━[/bold {queue_style}]")

            total = 0
            for status in status_order:
                if status in queue_data:
                    count = queue_data[status]
                    style = self._status_style(status)
                    lines.append(f"  [{style}]{status:<12}[/{style}] {count:>6}")
                    total += count

            lines.append(f"  [dim]{'─' * 18}[/dim]")
            lines.append(f"  [bold]{'Total':<12}[/bold] {total:>6}")
            lines.append("")

        self.update("\n".join(lines).rstrip())

    def _status_style(self, status: str) -> str:
        return {
            "completed": "green",
            "failed": "red",
            "pending": "yellow",
            "processing": "blue",
            "skipped": "dim",
        }.get(status, "white")


class SiteStatsTable(DataTable):
    """Table showing stats by site grouped by queue type."""

    def on_mount(self) -> None:
        self.add_columns("Queue", "Site", "Pend", "Proc", "Done", "Fail")
        self.cursor_type = "row"

    def update_data(self, rows: list) -> None:
        """Update with pre-fetched data."""
        # Aggregate by queue_name and site_id
        stats: dict[tuple[str, str], dict[str, int]] = {}
        for row in rows:
            key = (row["queue_name"], row["site_id"] or "unknown")
            if key not in stats:
                stats[key] = {"pending": 0, "processing": 0, "completed": 0, "failed": 0}
            status = row["status"]
            if status in stats[key]:
                stats[key][status] += 1

        # Sort by queue_name, then by pending+processing desc
        sorted_keys = sorted(stats.keys(), key=lambda k: (k[0], -(stats[k]["pending"] + stats[k]["processing"])))

        self.clear()
        for queue_name, site_id in sorted_keys:
            s = stats[(queue_name, site_id)]
            queue_style = "[cyan]" if queue_name == "listing" else "[magenta]"
            self.add_row(
                f"{queue_style}{queue_name[:7]}[/]",
                site_id[:15],
                str(s["pending"]) if s["pending"] else "-",
                str(s["processing"]) if s["processing"] else "-",
                str(s["completed"]) if s["completed"] else "-",
                str(s["failed"]) if s["failed"] else "-",
            )


class FailedItemsTable(DataTable):
    """Table showing failed items."""

    def on_mount(self) -> None:
        self.add_columns("Queue", "Site", "URL", "Error")
        self.cursor_type = "row"

    def update_data(self, rows: list) -> None:
        """Update with pre-fetched data (already filtered to failed)."""
        self.clear()
        for row in rows[:15]:  # Limit to 15
            queue_name = row["queue_name"]
            site_id = row["site_id"] or "?"
            url = row["url"]
            error = row["error"] or "No error"
            queue_style = "[cyan]" if queue_name == "listing" else "[magenta]"
            self.add_row(
                f"{queue_style}{queue_name[:7]}[/]",
                site_id[:12],
                url[:45] + "..." if len(url) > 45 else url,
                error[:30],
            )


class ProcessingItemsTable(DataTable):
    """Table showing currently processing items."""

    def on_mount(self) -> None:
        self.add_columns("Queue", "Site", "URL")
        self.cursor_type = "row"

    def update_data(self, rows: list) -> None:
        """Update with pre-fetched data (already filtered to processing)."""
        self.clear()
        for row in rows[:10]:  # Limit to 10
            queue_name = row["queue_name"]
            site_id = row["site_id"] or "?"
            url = row["url"]
            queue_style = "[cyan]" if queue_name == "listing" else "[magenta]"
            self.add_row(
                f"{queue_style}{queue_name[:7]}[/]",
                site_id[:12],
                url[:55] + "..." if len(url) > 55 else url,
            )


class DBOSTUIApp(App):
    """Full-screen DBOS monitoring TUI."""

    CSS = """
    Screen {
        layout: grid;
        grid-size: 2 2;
        grid-columns: 1fr 2fr;
        grid-rows: auto 1fr;
        background: $surface;
    }

    Header {
        background: $primary-darken-3;
        color: $text;
    }

    Footer {
        background: $primary-darken-3;
    }

    #left-panel {
        layout: vertical;
        height: auto;
    }

    #summary-container {
        height: auto;
        border: heavy $success;
        border-title-color: $success-lighten-2;
        padding: 1;
        background: $surface-darken-1;
    }

    #site-stats-container {
        height: auto;
        max-height: 14;
        border: heavy $primary;
        border-title-color: $primary-lighten-2;
        background: $surface-darken-1;
    }

    #right-panel {
        layout: vertical;
        height: auto;
    }

    #failed-container {
        height: auto;
        max-height: 10;
        border: heavy $error;
        border-title-color: $error-lighten-2;
        background: $surface-darken-1;
    }

    #processing-container {
        height: auto;
        max-height: 8;
        border: heavy $accent;
        border-title-color: $accent-lighten-2;
        background: $surface-darken-1;
    }

    #log-container {
        column-span: 2;
        border: heavy $secondary;
        border-title-color: $secondary-lighten-2;
        background: $surface-darken-2;
    }

    #log-tabs {
        height: 100%;
    }

    #log-tabs > TabPane {
        padding: 0;
    }

    #log-scroll, #error-scroll {
        height: 100%;
        background: transparent;
    }

    /* Style the error tab count indicator */
    .error-count {
        color: $error;
        text-style: bold;
    }

    .panel-title {
        text-style: bold;
        padding: 0 1;
        color: $text-muted;
    }

    DataTable {
        height: auto;
        background: transparent;
    }

    DataTable > .datatable--header {
        background: $primary-darken-2;
        color: $text;
        text-style: bold;
    }

    DataTable > .datatable--cursor {
        background: $primary;
        color: $text;
    }

    DataTable > .datatable--hover {
        background: $primary-darken-1;
    }

    ReverseLogWidget {
        height: auto;
        background: transparent;
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("r", "refresh", "Refresh"),
        ("e", "show_errors", "Errors"),
        ("l", "show_logs", "Logs"),
    ]

    process_running = reactive(False)

    def __init__(self, db_path: Path, cmd: list[str] | None = None):
        super().__init__()
        self.db_path = db_path
        self.cmd = cmd
        self.process: subprocess.Popen | None = None
        self.exit_code: int | None = None
        self._stop_threads = False
        self._error_count = 0

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)

        with Vertical(id="left-panel"):
            with Container(id="summary-container"):
                yield Static("Queue Summary", classes="panel-title")
                yield QueueSummaryWidget(id="summary")

            with Container(id="site-stats-container"):
                yield Static("Stats by Site", classes="panel-title")
                yield SiteStatsTable(id="site-stats-table")

        with Vertical(id="right-panel"):
            with Container(id="failed-container"):
                yield Static("Failed Items", classes="panel-title")
                yield FailedItemsTable(id="failed-table")

            with Container(id="processing-container"):
                yield Static("Processing Now", classes="panel-title")
                yield ProcessingItemsTable(id="processing-table")

        with Container(id="log-container"):
            with TabbedContent(id="log-tabs"):
                with TabPane("Logs", id="logs-tab"):
                    with VerticalScroll(id="log-scroll"):
                        yield ReverseLogWidget(id="log", max_lines=MAX_LOG_LINES)
                with TabPane("Errors (0)", id="errors-tab"):
                    with VerticalScroll(id="error-scroll"):
                        yield ReverseLogWidget(id="error-log", max_lines=MAX_LOG_LINES)

        yield Footer()

    def on_mount(self) -> None:
        self.refresh_all()
        self.set_interval(REFRESH_INTERVAL, self.refresh_all)

        # Set up SIGINT handler - kill process group directly, then exit
        def handle_sigint(signum, frame):
            self._stop_threads = True
            self._kill_process_group_sync()
            # Reset terminal and exit
            print("\033[?1049l", end="", flush=True)  # Exit alternate screen
            print("\033[?25h", end="", flush=True)  # Show cursor
            print("\n[Interrupted]")
            os._exit(130)

        signal.signal(signal.SIGINT, handle_sigint)

        if self.cmd:
            self.start_worker()

    def refresh_all(self) -> None:
        """Refresh all data displays with a single DB query."""
        # Single query to fetch all queue items
        conn = get_db_connection(self.db_path)
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT queue_name, site_id, status, url, error, updated_at
                    FROM queue_items
                    ORDER BY queue_name, updated_at DESC
                """)
                all_rows = [dict(row) for row in cursor.fetchall()]
                conn.close()

                # Distribute to widgets
                self.query_one("#summary", QueueSummaryWidget).update_data(all_rows)
                self.query_one("#site-stats-table", SiteStatsTable).update_data(all_rows)

                # Filter for specific statuses
                failed_rows = [r for r in all_rows if r["status"] == "failed"]
                processing_rows = [r for r in all_rows if r["status"] == "processing"]

                self.query_one("#failed-table", FailedItemsTable).update_data(failed_rows)
                self.query_one("#processing-table", ProcessingItemsTable).update_data(processing_rows)
            except sqlite3.Error:
                pass

        # Check process status
        if self.process and self.process.poll() is not None:
            self.exit_code = self.process.returncode
            self.process_running = False
            log = self.query_one("#log", ReverseLogWidget)
            log.write_line(f"[bold yellow]Process exited with code {self.exit_code}[/bold yellow]")

    def action_refresh(self) -> None:
        """Manual refresh action."""
        self.refresh_all()

    def action_show_errors(self) -> None:
        """Switch to errors tab."""
        try:
            tabs = self.query_one("#log-tabs", TabbedContent)
            tabs.active = "errors-tab"
        except Exception:
            pass

    def action_show_logs(self) -> None:
        """Switch to logs tab."""
        try:
            tabs = self.query_one("#log-tabs", TabbedContent)
            tabs.active = "logs-tab"
        except Exception:
            pass

    def start_worker(self) -> None:
        """Start the worker subprocess."""
        if not self.cmd:
            return

        # Start in a new process group so we can kill all children on exit
        self.process = subprocess.Popen(
            self.cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        self.process_running = True

        # Start threads to read output
        stdout_thread = threading.Thread(
            target=self._read_output, args=(self.process.stdout, False), daemon=True
        )
        stderr_thread = threading.Thread(
            target=self._read_output, args=(self.process.stderr, True), daemon=True
        )
        stdout_thread.start()
        stderr_thread.start()

    def _read_output(self, pipe, is_stderr: bool = False) -> None:
        """Read output from subprocess and write to log."""
        prefix = "[stderr] " if is_stderr else ""
        try:
            for line in iter(pipe.readline, ""):
                if self._stop_threads:
                    break
                line = line.rstrip()
                if line:
                    self.call_from_thread(self._write_log, f"{prefix}{line}")
        except Exception:
            pass
        finally:
            pipe.close()

    def _write_log(self, text: str) -> None:
        """Write a line to the log widget (must be called from main thread)."""
        try:
            log = self.query_one("#log", ReverseLogWidget)
            # Add color coding for common log patterns
            styled_text = self._style_log_line(text)
            log.write_line(styled_text)

            # Check if this is an error/exception and write to error log
            if self._is_error_line(text):
                error_log = self.query_one("#error-log", ReverseLogWidget)
                error_log.write_line(styled_text)
                self._error_count += 1
                self._update_error_tab_title()
        except Exception:
            pass

    def _is_error_line(self, text: str) -> bool:
        """Detect if a log line represents an error or exception."""
        text_lower = text.lower()
        # Check for common error patterns
        error_patterns = [
            "error",
            "exception",
            "traceback",
            "failed",
            "failure",
            "critical",
            "fatal",
            "[stderr]",
            "raise ",
            "assert",
        ]
        return any(pattern in text_lower for pattern in error_patterns)

    def _update_error_tab_title(self) -> None:
        """Update the error tab title with the current error count."""
        try:
            tabs = self.query_one("#log-tabs", TabbedContent)
            errors_tab = self.query_one("#errors-tab", TabPane)
            # Update the tab label
            new_label = f"Errors ({self._error_count})"
            tabs.get_tab("errors-tab").label = new_label
        except Exception:
            pass

    def _style_log_line(self, text: str) -> str:
        """Apply color styling to log lines, converting ANSI codes to Rich markup."""
        # Handle stderr prefix specially
        if text.startswith("[stderr]"):
            # Extract the prefix and convert the rest
            prefix = "[stderr] "
            rest = text[len("[stderr]") :].lstrip()
            converted = ansi_to_rich(rest)
            # Wrap stderr output in red if it has no colors
            if "[" not in converted or converted == escape(rest):
                return f"[red]{prefix}{converted}[/red]"
            return f"[red]{prefix}[/red]{converted}"

        # Convert ANSI codes to Rich markup
        converted = ansi_to_rich(text)

        # If no ANSI colors were found, apply heuristic coloring
        has_rich_tags = "[" in converted and "]" in converted and converted != escape(text)
        if not has_rich_tags:
            # Quick check - only scan if likely to match
            text_lower = text.lower()
            if "error" in text_lower or "fail" in text_lower:
                return f"[red]{converted}[/red]"
            if "warn" in text_lower:
                return f"[yellow]{converted}[/yellow]"

        return converted

    def _kill_process_group_sync(self) -> None:
        """Kill the process group synchronously (for signal handlers)."""
        if not self.process:
            return
        try:
            pgid = os.getpgid(self.process.pid)
            os.killpg(pgid, signal.SIGKILL)
        except (ProcessLookupError, OSError):
            pass

    def _kill_process_tree(self) -> None:
        """Kill the process and all its children via process group (non-blocking)."""
        if not self.process or self.process.poll() is not None:
            return

        try:
            pgid = os.getpgid(self.process.pid)
            # Send SIGTERM first for graceful shutdown
            os.killpg(pgid, signal.SIGTERM)

            # Schedule a force-kill after a short delay if still running
            def force_kill():
                if self.process and self.process.poll() is None:
                    try:
                        os.killpg(pgid, signal.SIGKILL)
                    except (ProcessLookupError, OSError):
                        pass

            # Use a timer thread to avoid blocking
            timer = threading.Timer(1.0, force_kill)
            timer.daemon = True
            timer.start()

        except (ProcessLookupError, OSError):
            pass

    def action_quit(self) -> None:
        """Quit the application."""
        self._stop_threads = True

        # Close pipes first to unblock reader threads
        if self.process:
            try:
                if self.process.stdout:
                    self.process.stdout.close()
                if self.process.stderr:
                    self.process.stderr.close()
            except Exception:
                pass

        self._kill_process_tree()

        # Force exit - use _exit to avoid blocking on Textual cleanup
        # Reset terminal first
        print("\033[?1049l", end="", flush=True)  # Exit alternate screen
        print("\033[?25h", end="", flush=True)  # Show cursor
        print("\n[Quit]")

        os._exit(0)


def print_final_status(db_path: Path) -> None:
    """Print final queue status after TUI exits."""
    if not db_path.exists():
        print("No database found")
        return

    try:
        conn = sqlite3.connect(str(db_path), timeout=1.0)
        cursor = conn.cursor()

        print("\n" + "=" * 70)
        print("FINAL QUEUE STATUS")
        print("=" * 70)

        # Status counts by queue type
        for queue_name in ["listing", "detail"]:
            cursor.execute(
                "SELECT status, COUNT(*) FROM queue_items WHERE queue_name = ? GROUP BY status ORDER BY status",
                (queue_name,),
            )
            rows = cursor.fetchall()
            if rows:
                print(f"\n[{queue_name.upper()}]")
                for status, count in rows:
                    print(f"  {status:<15} {count:>10}")
                cursor.execute(
                    "SELECT COUNT(*) FROM queue_items WHERE queue_name = ?",
                    (queue_name,),
                )
                total = cursor.fetchone()[0]
                print(f"  {'-' * 25}")
                print(f"  {'Total':<15} {total:>10}")

        # Failed URLs
        cursor.execute("""
            SELECT queue_name, site_id, url, error FROM queue_items
            WHERE status = 'failed' ORDER BY queue_name, site_id
        """)
        failed = cursor.fetchall()
        if failed:
            print("\n" + "-" * 70)
            print("FAILED URLs:")
            for queue_name, site_id, url, error in failed:
                print(f"  [{queue_name}:{site_id or 'unknown'}] {url}")
                if error:
                    print(f"    Error: {error[:80]}")

        # Pending URLs
        cursor.execute("""
            SELECT queue_name, site_id, url FROM queue_items
            WHERE status = 'pending' ORDER BY queue_name, site_id
        """)
        pending = cursor.fetchall()
        if pending:
            print("\n" + "-" * 70)
            print("PENDING URLs:")
            for queue_name, site_id, url in pending:
                print(f"  [{queue_name}:{site_id or 'unknown'}] {url}")

        # Processing URLs
        cursor.execute("""
            SELECT queue_name, site_id, url FROM queue_items
            WHERE status = 'processing' ORDER BY queue_name, site_id
        """)
        processing = cursor.fetchall()
        if processing:
            print("\n" + "-" * 70)
            print("PROCESSING URLs:")
            for queue_name, site_id, url in processing:
                print(f"  [{queue_name}:{site_id or 'unknown'}] {url}")

        print("\n" + "=" * 70)
        conn.close()
    except sqlite3.Error as e:
        print(f"Database error: {e}")


def main():
    parser = argparse.ArgumentParser(description="DBOS Worker TUI")
    parser.add_argument("--db", type=Path, help="Path to DBOS SQLite database")
    parser.add_argument("--cmd", nargs=argparse.REMAINDER, help="Command to run")
    args = parser.parse_args()

    # Resolve database path
    if args.db:
        db_path = args.db
    else:
        db_path = (
            Path(__file__).resolve().parent.parent.parent
            / "job_scrape_application"
            / "dbos_runtime"
            / "dbos.sqlite"
        )

    # Run the TUI app
    app = DBOSTUIApp(db_path, cmd=args.cmd if args.cmd else None)
    app.run()

    # Print final status after exit
    print_final_status(db_path)

    sys.exit(app.exit_code or 0)


if __name__ == "__main__":
    main()
