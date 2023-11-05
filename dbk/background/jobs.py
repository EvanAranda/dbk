from .worker_pool import Job


def _sync_data_sources_job(conn_id):
    import time

    print("calling sync_data_sources_job")
    time.sleep(2)
    print("sync_data_sources_job done")
    return f"Synced {conn_id}"


def sync_data_sources(conn_id: int) -> Job[str]:
    return Job(_sync_data_sources_job, conn_id)
