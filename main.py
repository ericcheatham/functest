import hashlib
import logging
import sys
import pdb

from turbine.src.turbine_app import RecordList, TurbineApp, Records

logging.basicConfig(level=logging.INFO)


def anonymize(records: RecordList) -> RecordList:
    return records


class App:
    @staticmethod
    async def run(turbine: TurbineApp):
        try:

            source = await turbine.resources("db1")


            records = await source.records("*", {})

            # Specify which secrets in environment variables should be passed
            # into the Process.
            # Replace 'PWD' with the name of the environment variable.
            #
            # turbine.register_secrets("PWD")

            # Specify what code to execute against upstream records
            # with the `process` function.
            # Replace `anonymize` with the name of your function code.
            # anonymized = await turbine.process(records, anonymize)

            # Identify a downstream data store for your data app
            # with the `resources` function.
            # Replace `destination_name` with the resource name the
            # data store was configured with on the Meroxa platform.
            destination_db = await turbine.resources("cheatham-s3")        
            await destination_db.write(records , "collection_archive", {})
        except Exception as e:
            print(e, file=sys.stderr)
