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
from pathlib import Path

from rich.markup import escape
from textual.app import App, ComposeResult
from textual.containers import Container, Vertical
from textual.reactive import reactive
from textual.widgets import DataTable, Footer, Header, RichLog, Static, TabbedContent, TabPane

# Regex to match ANSI escape codes (SGR and other sequences)
ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]|\x1b\][^\x07]*\x07")
# Pattern to match SGR sequences: ESC [ params m (pre-compiled for performance)
ANSI_SGR_RE = re.compile(r"\x1b\[([0-9;]*)m")

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
    result = []
    open_tags: list[str] = []  # Stack of open Rich tags
    last_end = 0

    for match in ANSI_SGR_RE.finditer(text):
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

# Log level detection patterns (pre-defined for performance)
FATAL_PATTERNS = frozenset(["fatal", "critical", "panic", "abort"])
ERROR_PATTERNS = frozenset(["error", "exception", "traceback", "failed", "failure", "[stderr]", "raise "])
WARN_PATTERNS = frozenset(["warn", "warning", "deprecated", "caution"])


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
        height: 1fr;
        border: heavy $secondary;
        border-title-color: $secondary-lighten-2;
        background: $surface-darken-2;
    }

    #log-tabs {
        height: 1fr;
    }

    #log-tabs ContentSwitcher {
        height: 1fr;
    }

    #log-tabs > TabPane {
        padding: 0;
        height: 1fr;
    }

    RichLog {
        height: 1fr;
        width: 100%;
        background: transparent;
        scrollbar-gutter: stable;
        padding: 0 1;
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
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("r", "refresh", "Refresh"),
        ("1", "show_info", "Info"),
        ("2", "show_warn", "Warn"),
        ("3", "show_error", "Error"),
        ("4", "show_fatal", "Fatal"),
    ]

    process_running = reactive(False)

    def __init__(self, db_path: Path, cmd: list[str] | None = None):
        super().__init__()
        self.db_path = db_path
        self.cmd = cmd
        self.process: subprocess.Popen | None = None
        self.exit_code: int | None = None
        self._stop_threads = False
        self._log_counts: dict[str, int] = {"info": 0, "warn": 0, "error": 0, "fatal": 0}
        self._tab_titles_dirty: set[str] = set()
        # Cached widget references (populated on mount)
        self._log_widgets: dict[str, RichLog] = {}
        self._tabs: TabbedContent | None = None
        self._summary_widget: QueueSummaryWidget | None = None
        self._site_stats_table: SiteStatsTable | None = None
        self._failed_table: FailedItemsTable | None = None
        self._processing_table: ProcessingItemsTable | None = None
        # Persistent read-only DB connection
        self._db_conn: sqlite3.Connection | None = None

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
                with TabPane("Info (0)", id="info-tab"):
                    yield RichLog(id="info-log", max_lines=MAX_LOG_LINES, markup=True, auto_scroll=True)
                with TabPane("Warn (0)", id="warn-tab"):
                    yield RichLog(id="warn-log", max_lines=MAX_LOG_LINES, markup=True, auto_scroll=True)
                with TabPane("Error (0)", id="error-tab"):
                    yield RichLog(id="error-log", max_lines=MAX_LOG_LINES, markup=True, auto_scroll=True)
                with TabPane("Fatal (0)", id="fatal-tab"):
                    yield RichLog(id="fatal-log", max_lines=MAX_LOG_LINES, markup=True, auto_scroll=True)

        yield Footer()

    def on_mount(self) -> None:
        # Cache widget references to avoid query_one on every update
        self._log_widgets = {
            "info": self.query_one("#info-log", RichLog),
            "warn": self.query_one("#warn-log", RichLog),
            "error": self.query_one("#error-log", RichLog),
            "fatal": self.query_one("#fatal-log", RichLog),
        }
        self._tabs = self.query_one("#log-tabs", TabbedContent)
        self._summary_widget = self.query_one("#summary", QueueSummaryWidget)
        self._site_stats_table = self.query_one("#site-stats-table", SiteStatsTable)
        self._failed_table = self.query_one("#failed-table", FailedItemsTable)
        self._processing_table = self.query_one("#processing-table", ProcessingItemsTable)

        self.refresh_all()
        self.set_interval(REFRESH_INTERVAL, self.refresh_all)
        # Batched tab title updates every 200ms
        self.set_interval(0.2, self._flush_tab_titles)

        # Set up SIGINT handler - close pipes, kill process, then exit immediately
        def handle_sigint(signum, frame):
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
            self._kill_process_group_sync()
            # Reset terminal and exit
            print("\033[?1049l", end="", flush=True)  # Exit alternate screen
            print("\033[?25h", end="", flush=True)  # Show cursor
            print("\n[Interrupted]")
            os._exit(130)

        signal.signal(signal.SIGINT, handle_sigint)

        if self.cmd:
            self.start_worker()

    def _get_db_connection(self) -> sqlite3.Connection | None:
        """Get or create a persistent read-only DB connection."""
        if self._db_conn is not None:
            return self._db_conn

        if not self.db_path.exists():
            return None

        try:
            # Read-only connection with URI mode
            self._db_conn = sqlite3.connect(
                f"file:{self.db_path}?mode=ro",
                uri=True,
                timeout=5.0,
                check_same_thread=False,
            )
            self._db_conn.row_factory = sqlite3.Row
            return self._db_conn
        except sqlite3.Error:
            return None

    def refresh_all(self) -> None:
        """Refresh all data displays with a single DB query."""
        conn = self._get_db_connection()
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT queue_name, site_id, status, url, error, updated_at
                    FROM queue_items
                    ORDER BY queue_name, updated_at DESC
                """)
                all_rows = [dict(row) for row in cursor.fetchall()]

                # Distribute to cached widgets
                if self._summary_widget:
                    self._summary_widget.update_data(all_rows)
                if self._site_stats_table:
                    self._site_stats_table.update_data(all_rows)

                # Filter for specific statuses
                failed_rows = [r for r in all_rows if r["status"] == "failed"]
                processing_rows = [r for r in all_rows if r["status"] == "processing"]

                if self._failed_table:
                    self._failed_table.update_data(failed_rows)
                if self._processing_table:
                    self._processing_table.update_data(processing_rows)
            except sqlite3.Error:
                # Connection may be stale, reset it
                self._db_conn = None

        # Check process status
        if self.process and self.process.poll() is not None:
            self.exit_code = self.process.returncode
            self.process_running = False
            info_log = self._log_widgets.get("info")
            if info_log:
                info_log.write(f"[bold yellow]Process exited with code {self.exit_code}[/bold yellow]")
                self._log_counts["info"] += 1
                self._tab_titles_dirty.add("info")

    def action_refresh(self) -> None:
        """Manual refresh action."""
        self.refresh_all()

    def action_show_info(self) -> None:
        """Switch to info tab."""
        if self._tabs:
            self._tabs.active = "info-tab"

    def action_show_warn(self) -> None:
        """Switch to warn tab."""
        if self._tabs:
            self._tabs.active = "warn-tab"

    def action_show_error(self) -> None:
        """Switch to error tab."""
        if self._tabs:
            self._tabs.active = "error-tab"

    def action_show_fatal(self) -> None:
        """Switch to fatal tab."""
        if self._tabs:
            self._tabs.active = "fatal-tab"

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
        """Write a line to the appropriate log widget based on severity."""
        try:
            text_lower = text.lower()
            styled_text = self._style_log_line(text, text_lower)
            level = self._detect_log_level(text_lower)

            # Route to appropriate log widget (use cached reference)
            log_widget = self._log_widgets.get(level)
            if log_widget:
                log_widget.write(styled_text)
                self._log_counts[level] += 1
                self._tab_titles_dirty.add(level)  # Mark for batched update
        except Exception:
            pass

    def _detect_log_level(self, text_lower: str) -> str:
        """Detect log level from lowercase text. Returns 'info', 'warn', 'error', or 'fatal'."""
        # Fatal patterns (most severe)
        if any(p in text_lower for p in FATAL_PATTERNS):
            return "fatal"

        # Error patterns
        if any(p in text_lower for p in ERROR_PATTERNS):
            return "error"

        # Warning patterns
        if any(p in text_lower for p in WARN_PATTERNS):
            return "warn"

        # Default to info
        return "info"

    def _flush_tab_titles(self) -> None:
        """Batch update dirty tab titles."""
        if not self._tab_titles_dirty or not self._tabs:
            return
        label_names = {"info": "Info", "warn": "Warn", "error": "Error", "fatal": "Fatal"}
        for level in list(self._tab_titles_dirty):
            try:
                tab_id = f"{level}-tab"
                new_label = f"{label_names[level]} ({self._log_counts[level]})"
                self._tabs.get_tab(tab_id).label = new_label
            except Exception:
                pass
        self._tab_titles_dirty.clear()

    def _update_tab_title(self, level: str) -> None:
        """Mark a tab title for batched update."""
        self._tab_titles_dirty.add(level)

    def _style_log_line(self, text: str, text_lower: str) -> str:
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

        # Close DB connection
        if self._db_conn:
            try:
                self._db_conn.close()
            except Exception:
                pass

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
        conn = sqlite3.connect(str(db_path), timeout=5.0)
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
