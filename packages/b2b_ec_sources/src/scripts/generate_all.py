"""Generate all source datasets for seed and optional evolution cycles."""

import argparse

from b2b_ec_sources.marketing_leads import MarketingLeadsGenerator
from b2b_ec_sources.postgres_gen import CommerceSourceDataGenerator
from b2b_ec_sources.webserver_logs import WebLogGenerator
from rich.console import Console

console = Console()


def _run_single_cycle(cycle_index: int, total_cycles: int) -> None:
    # Keep source generation order stable so downstream alignment is predictable.
    if total_cycles > 1:
        console.print(f"[cyan]Run {cycle_index + 1}/{total_cycles}[/cyan]")

    console.print("[cyan]1/3[/cyan] Postgres source")
    CommerceSourceDataGenerator().generate()

    console.print("[cyan]2/3[/cyan] Webserver logs")
    WebLogGenerator().generate()

    console.print("[cyan]3/3[/cyan] Marketing leads")
    MarketingLeadsGenerator().generate()


def generate_all_sources(evolution_cycle: int = 0) -> None:
    """Run seed generation once plus N additional evolution cycles."""
    console.print("[bold]Generating source datasets...[/bold]")
    # One seed run + user-requested number of evolution runs.
    total_cycles = evolution_cycle + 1

    for cycle in range(total_cycles):
        _run_single_cycle(cycle, total_cycles)

    console.print("[bold green]Done.[/bold green] All sources generated.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate source datasets for seed and evolution cycles.")
    parser.add_argument(
        "--cycles",
        type=int,
        default=0,
        help="Number of evolution cycles to run after the seed cycle (default: 0).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.cycles < 0:
        raise ValueError("--cycles must be >= 0")
    generate_all_sources(evolution_cycle=args.cycles)
