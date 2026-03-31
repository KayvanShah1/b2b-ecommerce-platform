from rich.console import Console

from b2b_ec_sources.marketing_leads import MarketingLeadsGenerator
from b2b_ec_sources.postgres_gen import CommerceSourceDataGenerator
from b2b_ec_sources.webserver_logs import WebLogGenerator

console = Console()


def generate_all_sources() -> None:
    console.print("[bold]Generating source datasets...[/bold]")

    console.print("[cyan]1/3[/cyan] Postgres source")
    CommerceSourceDataGenerator().generate()

    console.print("[cyan]2/3[/cyan] Webserver logs")
    WebLogGenerator().generate()

    console.print("[cyan]3/3[/cyan] Marketing leads")
    MarketingLeadsGenerator().generate()

    console.print("[bold green]Done.[/bold green] All sources generated.")

if __name__ == "__main__":
    generate_all_sources()
