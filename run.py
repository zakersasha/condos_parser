"""Initialization of Flask app."""
import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from newlauncher.parse import parse_new_launcher

if __name__ == '__main__':
    schedule = AsyncIOScheduler()
    # New launcher
    schedule.add_job(parse_new_launcher, 'cron', hour=12)

    schedule.start()

    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
