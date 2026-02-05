"""
Azure Function for Events Refresh.

Timer-triggered function that runs every 5 minutes to fetch new events
from Polymarket API and write to bronze layer.
"""
import logging
import azure.functions as func

# Configure logging before imports
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("events_refresh")
logger.setLevel(logging.DEBUG)

# Quiet noisy libraries
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

app = func.FunctionApp()


@app.timer_trigger(schedule="0 */5 * * * *", arg_name="timer", run_on_startup=False)
def events_refresh_timer(timer: func.TimerRequest) -> None:
    """
    Timer-triggered function to refresh events from Polymarket API.
    Runs every 5 minutes.
    """
    from events_refresh_new.runner import run_refresh

    logger.info("=== EVENTS REFRESH START ===")

    if timer.past_due:
        logger.warning("Timer is past due, running anyway")

    try:
        result = run_refresh(verbose=True)
        logger.info(
            f"=== EVENTS REFRESH COMPLETE === "
            f"records_fetched={result['total_records']} "
            f"offsets_processed={result['offsets_processed']}"
        )
    except Exception as e:
        logger.exception(f"=== EVENTS REFRESH FAILED === error={e}")
        raise
