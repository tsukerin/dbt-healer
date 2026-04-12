import os
import subprocess
import time
import typer
from pathlib import Path
from rich.markdown import Markdown
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
import questionary

from common.config import get_config
from common.exceptions import CIFileExistsError, CIProfileExistsError
from app.ci_generator import GithubCIGenerator
from app.provider_builder import ProviderType, build_provider

console = Console()
config = get_config()

app = typer.Typer(help="DBT Healer CLI")

@app.command(help="Setup your enviroment for dbt-healer")
def setup():
    config_dict = {}
    first_setup = True
    answer = ""

    if Path(".env").exists():
        first_setup = False

        console.print("[yellow]Warning: .env file already exists.[/yellow]")
        time.sleep(2)
        answer = questionary.select(
            "Proceed with full setup or select the current step? (Full setup will fully replace the .env file)",
            choices=["Full setup", "Current step", "Exit from setup"],
        ).ask()
        
        if answer == "Exit from setup":
            return

    change_parameter = None
    if answer == 'Current step':
        change_parameter = questionary.select(
                "Which parameter do you want to overwrite?",
                choices=[
                    "Analyze url", 
                    "Repository path", 
                    "Name of dbt project", 
                    "Telegram Bot Token", 
                    "AI Provider and Token", 
                    "Git platform and git parameters",
                ],
        ).ask()
    else:
        first_setup = True

    if change_parameter == 'Analyze url' or first_setup:
        console.print(Markdown("1. Write the URL where errors from failed CI pipelines will be received (default is `localhost`):"))
        config_dict["service_endpoint"] = input(">>> ")

    if change_parameter == 'Name of dbt project' or first_setup:
        console.print(Markdown("2. What is the name of your dbt project? (e.g., `my_dbt_project`)"))
        config_dict["dbt_project_name"] = input(">>> ")

    if not change_parameter:
        answer = questionary.select(
            "Configure Telegram notifications?",
            choices=["Yes", "No"],
        ).ask()

    if (answer == "Yes" and first_setup) or change_parameter == 'Telegram Bot Token':
        console.print(Markdown("3. Telegram Bot Token: (needed for notifications, optional)"))
        config_dict["telegram_bot_token"] = input(">>> ")

    if change_parameter == 'AI Provider and Token' or first_setup:
        console.print(Markdown("4. What provider do you want to use for AI analysis?"))
        answer = questionary.select(
            "Select AI provider:",
            choices=["Google AI Studio", "Ollama"],
        ).ask()

        console.print(Markdown("5. Enter your API key for the selected AI provider:"))
        config_dict["ai_api_key"] = input(">>> ")
        config_dict["ai_provider"] = answer
        config.save({"ai_api_key": config_dict["ai_api_key"]})

        provider = build_provider(ai_provider=ProviderType(answer))

        console.print(Markdown("6. Which model do you prefer to use?"))
        answer = questionary.select(
            "Select model:",
            choices=provider.get_models_list(),
        ).ask()
        config_dict["ai_model"] = answer

    if change_parameter == 'Git platform and git parameters' or first_setup:
        console.print(Markdown("7. Which git platform do you use?"))
        answer = questionary.select(
            "Select platform:",
            choices=['Github'],
        ).ask()
        config_dict["git_platform"] = answer

        console.print(Markdown(f"8. Enter the {answer} repository link:"))
        config_dict["github_repo_link"] = input(">>> ")

        console.print(Markdown(f"9. Enter the base branch of your {answer} repository (e.g., `master` or `main`):"))
        config_dict["base_branch"] = input(">>> ")

        console.print(Markdown(f"10. Enter your {answer} token (needed for creating pull requests):"))
        config_dict["github_token"] = input(">>> ")
        config.save({"github_token": config_dict["github_token"]})

        console.print(Markdown(f"11. Enter your full local path of your {answer} repository:"))
        config_dict["full_path_to_repo_str"] = input(">>> ")

    console.print(Markdown("Saving configuration..."))
    time.sleep(1)

    if config.save(config_dict):
        console.print("[green]Configuration saved successfully![/green]" \
        "\nIf you want to change any value, you can edit the .env file in the project root directory.")
        time.sleep(1)

    if change_parameter == 'Git platform and git parameters' or first_setup:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            if answer == 'Github':
                generator = GithubCIGenerator()

            try:
                create_ci_file = progress.add_task(description="Creating CI into your repository...", total=None)
                generator.create_ci_file()
                time.sleep(2)
                progress.update(create_ci_file, completed=1)

                create_ci_profile = progress.add_task(description="Creating CI profile into your dbt project...", total=None)
                generator.create_ci_profile()
                time.sleep(2)
                progress.update(create_ci_profile, completed=1)

                console.print("[green]CI configured successfully![/green]")

            except CIFileExistsError:
                console.print("[yellow]CI file already exists. Skipping creation...[/yellow]")

            except CIProfileExistsError:
                console.print("[yellow]CI profile already exists. Skipping creation...[/yellow]")
                    
        

@app.command(help="Serve dbt-healer analyzer")
def serve(port: int = 8888):
    console.print(Markdown(f"Starting **dbt-healer** server on port {port}..."))
    config = get_config()

    config.save({"service_port": str(port)}) 
    
    cmd = ["docker", "compose", "up"]

    subprocess.run(cmd, check=True)

    

if __name__ == "__main__":
    app()
