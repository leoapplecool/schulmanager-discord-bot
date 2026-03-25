# Schulmanager Discord Bot

> Discord bot for the [Schulmanager API](https://github.com/leoapplecool/schulmanager-api) — automatically syncs school data into private per-user Discord channels.

![Python](https://img.shields.io/badge/Python-3.11%2B-blue?logo=python)
![Discord.py](https://img.shields.io/badge/discord.py-2.x-5865F2?logo=discord)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)

---

## Features

- **Private channels per user** — schedule feed, homework, grades, events, absences, messages, webhooks
- **Automatic sync** with configurable interval
- **Homework reactions** — react with ✅ to mark homework done
- **Daily digest** — morning summary in the status channel
- **Reminders** — DM reminders for upcoming exams and homework
- **Schedule change DMs** — instant notification for cancellations/substitutions
- **Admin commands** — manage all users from Discord

---

## Quick Start

### Docker (recommended)

```bash
git clone https://github.com/leoapplecool/schulmanager-discord-bot.git
cd schulmanager-discord-bot

cp .env.example .env
# Set SM_DISCORD_BOT_TOKEN and SM_DISCORD_API_BASE_URL in .env

docker compose up --build
```

### Local Development

```bash
pip install -e ".[dev]"
python -m schulmanager_discord_bot
```

---

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `SM_DISCORD_BOT_TOKEN` | Discord bot token | _(required)_ |
| `SM_DISCORD_API_BASE_URL` | URL of the Schulmanager API | `http://127.0.0.1:8000` |
| `SM_DISCORD_SYNC_INTERVAL_SECONDS` | Sync interval in seconds | `120` |
| `SM_DISCORD_DB_PATH` | Path to SQLite database | `data/discord_bot.sqlite3` |
| `SM_DISCORD_GUILD_ID` | Discord server ID (optional, for faster command sync) | _(empty)_ |
| `SM_DISCORD_TIMEZONE` | Timezone for display | `Europe/Berlin` |
| `SM_DISCORD_CATEGORY_PREFIX` | Prefix for private categories | `schulmanager` |
| `SM_DISCORD_DIGEST_TIME` | Time for daily digest (HH:MM) | `07:00` |
| `SM_DISCORD_DIGEST_ENABLED` | Enable daily digest | `true` |

---

## Slash Commands

| Command | Description |
|---|---|
| `/login email password [student_id]` | Log in and create private channels |
| `/logout [delete_category]` | Remove bot access |
| `/sync` | Manual sync |
| `/status` | Bot status for your account |
| `/calendar` | Send ICS calendar as DM |
| `/digest` | Show daily digest now |
| `/info` | General bot information |
| `/channels` | Show your Schulmanager channels |
| `/remind exams <hours>` | Exam reminder X hours before |
| `/remind homework <hours>` | Homework reminder X hours before |
| `/remind off <type>` | Disable a reminder |
| `/notify schedule-changes <on/off>` | DM on schedule changes |
| `/notify digest <on/off>` | Enable/disable daily digest |
| `/notify status` | Show notification settings |
| `/debug-state` | Debug info for your account |
| `/debug-webhook` | Send test message to webhook channel |
| `/admin-users` | List all bot users _(Admin)_ |
| `/admin-sync-all` | Sync all active users _(Admin)_ |
| `/admin-user-active` | Set user active/inactive _(Admin)_ |
| `/admin-errors` | Last sync errors _(Admin)_ |
| `/admin-stats` | Bot statistics _(Admin)_ |
| `/admin-purge` | Fully remove a user workspace _(Admin)_ |
| `/admin-flush-cache` | Flush API cache _(Admin)_ |

---

## Channel Layout

One private category per user with:

| Channel | Content |
|---|---|
| `00-status` | Sync status + daily digest + 🔄 sync button |
| `01-schedule-feed` | Upcoming lessons (auto-updated) |
| `02-schedule-week` | Weekly overview (one embed per day) |
| `03-homework` | One message per homework item + ✅ reaction |
| `04-grades` | Grades per subject + statistics |
| `05-events` | School events + next event panel |
| `06-webhooks` | Change log after each sync |
| `07-absences` | Absence overview |
| `08-messages` | School messages / inbox |

---

## License

MIT
