"""Initialization of Flask app."""
import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from dubai.main import parse_dubai
from miami.parser import parse_miami

from newlauncher.parse import parse_new_launcher
from newlauncher_sg.parse import parse_new_launcher_sg
from onthemarket.parser import parse_uk
from postgres.main import postgres_integration, call_overall_scripts

if __name__ == '__main__':
    schedule = AsyncIOScheduler({'apscheduler.job_defaults.max_instances': 2})

    # New launcher
    schedule.add_job(parse_new_launcher, 'cron', hour=4)

    # Srx
    # schedule.add_job(parse_srx, 'cron', hour=16, minute=14)

    # New launcher.sg
    schedule.add_job(parse_new_launcher_sg, 'cron', hour=5, minute=30)

    schedule.add_job(parse_miami, 'cron', hour=7)
    schedule.add_job(parse_uk, 'cron', hour=8, minute=30)
    schedule.add_job(parse_dubai, 'cron', hour=11, minute=30)

    # Postgres recording
    schedule.add_job(call_overall_scripts, 'cron', hour=14)
    schedule.add_job(postgres_integration, 'cron', hour=16)

    schedule.start()

    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
