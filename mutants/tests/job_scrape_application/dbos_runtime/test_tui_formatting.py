"""Tests for TUI log formatting and ANSI to Rich conversion."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _find_project_root() -> Path:
    """Find project root by looking for pyproject.toml."""
    # Try from cwd first (where pytest runs)
    cwd = Path.cwd()
    if (cwd / "pyproject.toml").exists():
        return cwd

    # Fall back to __file__ based approach
    path = Path(__file__).resolve()
    for parent in path.parents:
        if (parent / "pyproject.toml").exists():
            return parent
    raise RuntimeError("Could not find project root")


# Load dbos_tui module from mise-tasks/lib
_project_root = _find_project_root()
_module_path = _project_root / ".mise-tasks" / "lib" / "dbos_tui.py"
_spec = importlib.util.spec_from_file_location("dbos_tui", _module_path)
_dbos_tui = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_dbos_tui)

ansi_to_rich = _dbos_tui.ansi_to_rich


class TestAnsiToRich:
    """Tests for the ansi_to_rich function."""

    def test_plain_text_unchanged(self) -> None:
        """Plain text without ANSI codes should pass through escaped."""
        assert ansi_to_rich("Hello world") == "Hello world"

    def test_plain_text_with_brackets_escaped(self) -> None:
        """Rich markup characters in plain text should be escaped."""
        # Rich's escape() escapes [ but ] is only escaped when it would close a tag
        assert ansi_to_rich("value [key]") == r"value \[key]"
        assert ansi_to_rich("[bold]text[/bold]") == r"\[bold]text\[/bold]"

    def test_red_foreground(self) -> None:
        """ANSI red foreground should convert to Rich red tag."""
        # \x1b[31m = red foreground, \x1b[0m = reset
        result = ansi_to_rich("\x1b[31mError message\x1b[0m")
        assert result == "[red]Error message[/red]"

    def test_green_foreground(self) -> None:
        """ANSI green foreground should convert to Rich green tag."""
        result = ansi_to_rich("\x1b[32mSuccess\x1b[0m")
        assert result == "[green]Success[/green]"

    def test_yellow_foreground(self) -> None:
        """ANSI yellow foreground should convert to Rich yellow tag."""
        result = ansi_to_rich("\x1b[33mWarning\x1b[0m")
        assert result == "[yellow]Warning[/yellow]"

    def test_blue_foreground(self) -> None:
        """ANSI blue foreground should convert to Rich blue tag."""
        result = ansi_to_rich("\x1b[34mInfo\x1b[0m")
        assert result == "[blue]Info[/blue]"

    def test_bright_colors(self) -> None:
        """Bright ANSI colors should convert correctly."""
        result = ansi_to_rich("\x1b[91mBright red\x1b[0m")
        assert result == "[bright_red]Bright red[/bright_red]"

        result = ansi_to_rich("\x1b[92mBright green\x1b[0m")
        assert result == "[bright_green]Bright green[/bright_green]"

    def test_bold_style(self) -> None:
        """ANSI bold should convert to Rich bold tag."""
        result = ansi_to_rich("\x1b[1mBold text\x1b[0m")
        assert result == "[bold]Bold text[/bold]"

    def test_dim_style(self) -> None:
        """ANSI dim should convert to Rich dim tag."""
        result = ansi_to_rich("\x1b[2mDim text\x1b[0m")
        assert result == "[dim]Dim text[/dim]"

    def test_combined_bold_and_color(self) -> None:
        """Combined ANSI codes (e.g., bold red) should convert correctly."""
        # \x1b[1;31m = bold + red
        result = ansi_to_rich("\x1b[1;31mBold red error\x1b[0m")
        assert result == "[bold red]Bold red error[/bold red]"

    def test_sequential_colors(self) -> None:
        """Sequential color codes should be handled correctly."""
        result = ansi_to_rich("\x1b[31mRed\x1b[0m then \x1b[32mGreen\x1b[0m")
        assert result == "[red]Red[/red] then [green]Green[/green]"

    def test_nested_styles(self) -> None:
        """Nested styles accumulate and reset closes all at once."""
        result = ansi_to_rich("\x1b[1mBold \x1b[31mand red\x1b[0m")
        # Reset closes all open tags in reverse order (LIFO)
        assert result == "[bold]Bold [red]and red[/red][/bold]"

    def test_unclosed_tags_at_end(self) -> None:
        """Tags not closed with reset should be closed at end of string."""
        result = ansi_to_rich("\x1b[31mNo reset")
        assert result == "[red]No reset[/red]"

    def test_empty_sgr_is_reset(self) -> None:
        """Empty SGR sequence (ESC[m) should act as reset."""
        result = ansi_to_rich("\x1b[31mRed\x1b[m normal")
        assert result == "[red]Red[/red] normal"

    def test_non_sgr_sequences_stripped(self) -> None:
        """Non-SGR ANSI sequences should be stripped."""
        # \x1b[2J = clear screen, \x1b[H = cursor home
        result = ansi_to_rich("Start\x1b[2J\x1b[HEnd")
        assert result == "StartEnd"

    def test_text_with_special_chars(self) -> None:
        """Text with special characters should be properly escaped."""
        result = ansi_to_rich("\x1b[31mError: [object Object]\x1b[0m")
        # Rich's escape() escapes [ but ] is safe on its own
        assert result == r"[red]Error: \[object Object][/red]"

    def test_realistic_log_line(self) -> None:
        """Test with realistic log output format."""
        # Simulating structlog output
        log_line = "\x1b[2m2024-01-15 10:30:45\x1b[0m [\x1b[32minfo\x1b[0m] Processing job"
        result = ansi_to_rich(log_line)
        assert "[dim]2024-01-15 10:30:45[/dim]" in result
        assert "[green]info[/green]" in result
        assert "Processing job" in result

    def test_multiple_resets(self) -> None:
        """Multiple reset codes should not cause issues."""
        result = ansi_to_rich("\x1b[31mRed\x1b[0m\x1b[0m\x1b[0m")
        assert result == "[red]Red[/red]"


class TestAnsiToRichEdgeCases:
    """Edge case tests for ansi_to_rich function."""

    def test_empty_string(self) -> None:
        """Empty string should return empty string."""
        assert ansi_to_rich("") == ""

    def test_only_ansi_codes(self) -> None:
        """String with only ANSI codes returns empty Rich tags (renders as nothing)."""
        # Empty tags don't render anything visible, but are present in markup
        assert ansi_to_rich("\x1b[31m\x1b[0m") == "[red][/red]"

    def test_unknown_codes_ignored(self) -> None:
        """Unknown ANSI codes should be ignored but text preserved."""
        # \x1b[99m is not a standard code
        result = ansi_to_rich("\x1b[99mText\x1b[0m")
        assert result == "Text"

    def test_background_colors_ignored(self) -> None:
        """Background color codes should be ignored (not converted)."""
        # \x1b[41m = red background
        result = ansi_to_rich("\x1b[41mRed background\x1b[0m")
        assert result == "Red background"

    def test_foreground_with_background(self) -> None:
        """Foreground color should work even with background present."""
        # \x1b[31;41m = red foreground + red background
        result = ansi_to_rich("\x1b[31;41mRed on red\x1b[0m")
        assert result == "[red]Red on red[/red]"
