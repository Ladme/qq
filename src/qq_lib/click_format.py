# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import click
from click import HelpFormatter
from click_help_colors import HelpColorsCommand


class GNUHelpColorsCommand(HelpColorsCommand):
    """Custom formatter that prints options in GNU-style."""

    def get_help(self, ctx):
        class GNUHelpFormatter(HelpFormatter):
            def __init__(self, width=None, headers_color=None, options_color=None):
                super().__init__(width=width)
                self.headers_color = headers_color or "white"
                self.options_color = options_color or "white"

            def write_heading(self, heading):
                styled_heading = click.style(heading, fg=self.headers_color, bold=True)
                self.write(f"{styled_heading}\n")

            def write_usage(self, prog_name, args, prefix=None):
                """Override to make Usage: header bold"""
                if prefix is None:
                    prefix = "Usage:"

                styled_prefix = click.style(prefix, fg=self.headers_color, bold=True)
                usage_line = f"{styled_prefix} {prog_name}"

                if args:
                    usage_line += f" {args}"

                self.write(f"{usage_line}\n")

            def write_dl(self, rows, _col_max=30, _col_spacing=2):
                for term, definition in rows:
                    colored_term = click.style(term, fg=self.options_color, bold=True)
                    self.write(f"  {colored_term}\n")

                    if definition:
                        for line in definition.splitlines():
                            if line.strip():
                                self.write(f"      {line}\n")
                    self.write("\n")

        formatter = GNUHelpFormatter(
            width=ctx.terminal_width,
            headers_color=getattr(self, "help_headers_color", "white"),
            options_color=getattr(self, "help_options_color", "white"),
        )

        self.format_help(ctx, formatter)
        return formatter.getvalue()
