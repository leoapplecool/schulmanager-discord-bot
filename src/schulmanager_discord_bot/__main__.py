from schulmanager_discord_bot.config import get_settings
from schulmanager_discord_bot.bot import run_discord_bot


if __name__ == "__main__":
    run_discord_bot(get_settings())
