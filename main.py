import hashlib
import logging
import sys
import pdb

from turbine.src.turbine_app import RecordList, TurbineApp

logging.basicConfig(level=logging.INFO)


class App:
    @staticmethod
    async def run(turbine: TurbineApp):
        try:
            # To configure your data stores as resources on the Meroxa Platform
            # use the Meroxa Dashboard, CLI, or Meroxa Terraform Provider.
            # For more details refer to: https://docs.meroxa.com/

            # Identify an upstream data store for your data app
            # with the `resources` function.
            # Replace `source_name` with the resource name the
            # data store was configured with on the Meroxa platform.
            source = await turbine.resources("notion")

            # Specify which upstream records to pull
            # with the `records` function.
            # Replace `collection_name` with a table, collection,
            # or bucket name in your data store.
            # If you need additional connector configurations, replace '{}'
            # with the key and value, i.e. {"incrementing.field.name": "id"}
            records = await source.records("*", {})

            # Specify which secrets in environment variables should be passed
            # into the Process.
            # Replace 'PWD' with the name of the environment variable.
            #
            # turbine.register_secrets("PWD")

            destination_db = await turbine.resources("url")

            # Specify where to write records downstream
            # using the `write` function.
            # Replace `collection_archive` with a table, collection,
            # or bucket name in your data store.
            # If you need additional connector configurations, replace '{}'
            # with the key and value, i.e. {"behavior.on.null.values": "ignore"}
            await destination_db.write(records, "collection_archive", {})
        except Exception as e:
            print(e, file=sys.stderr)
