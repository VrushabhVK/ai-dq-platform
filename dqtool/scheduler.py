# dqtool/scheduler.py
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from .db import save_scan
from .profiling import profile_dataframe
from .scoring import compute_dq_score


scheduler = BackgroundScheduler()


def job_scan_dataframe(job_name: str, df):
    """
    Simple job: profile a DataFrame, compute DQ score, save result in DB.
    """
    if df is None or df.empty:
        print(f"[{job_name}] No data to scan.")
        return

    profile_df = profile_dataframe(df)
    score = compute_dq_score(profile_df)
    report = profile_df.to_dict(orient="records")

    save_scan(
        job_name=job_name,
        row_count=len(df),
        dq_score=score,
        report=report,
    )
    print(f"[{job_name}] Scan saved. rows={len(df)}, score={score}")


def schedule_job(job_name: str, cron: str, func, *args, **kwargs):
    """
    Schedule any function using a cron-like string: '0 2 * * *' (min hour day month dow)
    """
    parts = cron.split()
    if len(parts) != 5:
        raise ValueError("Cron must have exactly 5 fields: 'min hour day month dow'")

    trigger = CronTrigger(
        minute=parts[0],
        hour=parts[1],
        day=parts[2],
        month=parts[3],
        day_of_week=parts[4],
    )

    scheduler.add_job(
        func,
        trigger,
        args=args,
        kwargs=kwargs,
        id=job_name,
        replace_existing=True,
    )
    print(f"Scheduled job '{job_name}' with cron '{cron}'.")


def start_scheduler():
    """
    Start the APScheduler background scheduler.
    """
    if not scheduler.running:
        scheduler.start()
        print("Scheduler started.")
