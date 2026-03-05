import sys
import subprocess
import typer
from pathlib import Path
from rich.markdown import Markdown
from rich.console import Console
import questionary

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from common.config import get_config

console = Console()

app = typer.Typer(help="DBT Healer CLI")

@app.command(help="Setup your enviroment for dbt-healer")
def setup():
    config_dict = {}
    dotenv_file = BASE_DIR / ".env"
    if dotenv_file.exists():
        console.print("[yellow]Warning: .env file already exists. Skipping creation.[/yellow]")
        return

    console.print(Markdown("1. Enter the full path to your repository:"))
    config_dict["dbt_project_path"] = input(">>> ")

    console.print(Markdown("2. What is the name of your dbt project? (e.g., `my_dbt_project`)"))
    config_dict["dbt_project_name"] = input(">>> ")

    console.print(Markdown("3. Enter the GitHub repository link:"))
    config_dict["github_repo_link"] = input(">>> ")

    console.print(Markdown("4. Enter the base branch of your GitHub repository (e.g., `master` or `main`):"))
    config_dict["base_branch"] = input(">>> ")

    console.print(Markdown("5. Enter your database connection details (need for telegram notification. skip if you not need notifications):"))
    answer = questionary.select(
        "Configure Telegram notifications:",
        choices=["Yes", "No", "Exit"],
    ).ask()

    if answer == "Yes":
        console.print(Markdown("   - Telegram Bot Token: (needed for notifications, optional)"))
        config_dict["telegram_bot_token"] = input(">>> ")

    console.print(Markdown("6. What provider do you want to use for AI analysis?"))
    answer = questionary.select(
        "Select AI provider:",
        choices=["Google AI Studio", "Ollama", "Exit"],
    ).ask()

    console.print(Markdown("7. Enter your API key for the selected AI provider:"))
    config_dict["ai_api_key"] = input(">>> ")
    config_dict["ai_provider"] = answer

    console.print(Markdown("8. Enter your GitHub token (needed for creating pull requests):"))
    config_dict["github_token"] = input(">>> ")

    console.print(Markdown("Saving configuration..."))
    
    config = get_config()
    if config.save(config_dict):
        console.print("[green]Configuration saved successfully![/green]" \
        "\nIf you want to change any value, you can edit the .env file in the project root directory.")

@app.command(help="Serve dbt-healer analyzer")
def serve(port: int = 8888):
    console.print(Markdown(f"Starting **dbt-healer** server on port {port}..."))
    config = get_config()

    config.save({"service_port": str(port)}) 
    cmd = ["docker", "compose", "up", "--build"]

    subprocess.run(cmd, check=True)

    

if __name__ == "__main__":
    app()
