from b2b_ec_sources.marketing_leads import MarketingLeadsGenerator
from b2b_ec_sources.postgres_gen import CommerceSourceDataGenerator
from b2b_ec_sources.webserver_logs import WebLogGenerator
from rich.console import Console

console = Console()


def generate_all_sources(evolution_cycle=0) -> None:
    console.print("[bold]Generating source datasets...[/bold]")

    for cycle in range(evolution_cycle + 1):
        if evolution_cycle > 0:
            console.print(f"[cyan]Evolution Cycle {cycle + 1}[/cyan]")

        console.print("[cyan]1/3[/cyan] Postgres source")
        CommerceSourceDataGenerator().generate()

        console.print("[cyan]2/3[/cyan] Webserver logs")
        WebLogGenerator().generate()

        console.print("[cyan]3/3[/cyan] Marketing leads")
        MarketingLeadsGenerator().generate()

    console.print("[bold green]Done.[/bold green] All sources generated.")


if __name__ == "__main__":
    generate_all_sources(evolution_cycle=3)
