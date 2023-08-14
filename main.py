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

            # anonymized = await turbine.process(records, anonymize)

            destination_db = await turbine.resources("s3")        
            await destination_db.write(records , "collection_archive", {})
        except Exception as e:
            print(e, file=sys.stderr)
