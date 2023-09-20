"""Initialization of Flask app."""
import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from newlauncher.parse import parse_new_launcher
from newlauncher_sg.parse import parse_new_launcher_sg
from postgres.main import postgres_integration
from srx.parse import parse_srx

if __name__ == '__main__':
    schedule = AsyncIOScheduler()

    # New launcher
    schedule.add_job(parse_new_launcher, 'cron', hour=12)

    # Srx
    # schedule.add_job(parse_srx, 'cron', hour=16, minute=14)

    # New launcher.sg
    schedule.add_job(parse_new_launcher_sg, 'cron', hour=13, minute=30)

    # Postgres recording
    schedule.add_job(postgres_integration, 'cron', hour=16)

    schedule.start()

    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
