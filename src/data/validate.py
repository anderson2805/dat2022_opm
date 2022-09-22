#!/usr/bin/env python
import snowflake.connector
import configparser

config = configparser.ConfigParser()
config.read('SETTING.ini')


# Gets the version
ctx = snowflake.connector.connect(
    user=config['SNOWCONNECTOR']['USERNAME'],
    password=config['SNOWCONNECTOR']['PASSWORD'],
    account=config['SNOWCONNECTOR']['ACCOUNT']
    )
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
except Exception as e:
    print(e)
finally:
    cs.close()
ctx.close()

