from dagster import op, job, Definitions
from .main import main as app_main #import main


@op
def run_file_transfer():
   return app_main()

@job
def transfer_pl_files():
   run_file_transfer()

defs = Definitions(jobs=[transfer_pl_files])
