from dagster import op, job, Definitions
from .sbfmain import main as app_main #import main


@op
def run_sbf():
   return app_main()

@job
def extract_sbf_data():
   run_sbf()

defs = Definitions(jobs=[extract_sbf_data])
