#!/usr/bin/env python
import logging
logging.getLogger().setLevel(logging.INFO)

#https://docs.snowflake.com/en/user-guide/python-connector-example.html


def createJsonTable(cursor):
    result = cursor.execute(
        "CREATE OR REPLACE TABLE "
        "raw_table(src variant)")
    logging.info(result.fetchone())

    result = cursor.execute(
        """
        create or replace file format json_format
        type = 'json'
        strip_outer_array = true;
        """
    )
    logging.info(result.fetchone())