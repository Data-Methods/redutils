# from xmlrpc.client import ResponseError
# import json
# import time
# import os
# import base64
# import datetime
# import snowflake.connector
# from snowflake.connector.pandas_tools import write_pandas
# from cryptography.hazmat.primitives import serialization
# from cryptography.hazmat.backends import default_backend
# import requests
# from dateutil.relativedelta import relativedelta
# import pandas as pd
# import pyodbc
# from pyodbc import Error
# from secret_server import SecretServer


# class geneysys_load:
#     def __init__(self):
#         self.table = "LOAD_GEN_CONV"
#         self.script_name = "CUSTOM_LOAD_GEN_CONV"
#         self.ss_url = r"https://pamweb.inside.alaskausa.org/secretserver/winauthwebservices/api/v1/secrets/"
#         self.start = datetime.datetime.strptime(
#             "$PLoad_Gen_Conv_Start_Interval$", "%Y-%m-%dT%H:%M:%S.%f"
#         )
#         # self.end = datetime.datetime(2022, 7, 31)
#         self.end = datetime.datetime.today()
#         self.user_profile = os.environ["USERPROFILE"]
#         self.warehouse = "$PSnowflake_Warehouse$"
#         self.user = "$PSnowflake_User$"
#         self.database = "$PSnowflake_Database$"
#         self.schema_name = "STAGE"
#         self.account = "$PSnowflake_Account$"

#     def authorize(self):
#         """Get authorization info for snowflake"""
#         try:
#             with open(f"{self.user_profile}\\.snowsql\\rsa_key.p8", "rb") as key:
#                 p_key = serialization.load_pem_private_key(
#                     key.read(),
#                     password=os.environ["SNOWSQL_PRIVATE_KEY_PASSPHRASE"].encode(),
#                     backend=default_backend(),
#                 )

#             pkb = p_key.private_bytes(
#                 encoding=serialization.Encoding.DER,
#                 format=serialization.PrivateFormat.PKCS8,
#                 encryption_algorithm=serialization.NoEncryption(),
#             )

#             return pkb
#         except Exception as error:
#             print("Failed to authorize", error)
#             raise error

#     def create_snowflake_connection(self, pkb):
#         """Connect to the Snowflake database"""
#         try:
#             conn = snowflake.connector.connect(
#                 user=self.user,
#                 account=self.account,
#                 private_key=pkb,
#                 warehouse=self.warehouse,
#                 database=self.database,
#                 schema=self.schema_name,
#             )
#             return conn
#         except Exception as error:
#             print("Failed to connect to snowflake: ", error)
#             raise error

#     def create_table(self, conn, schema_name, table_name):
#         f"""Creating sql statement to create table {schema_name}.{table_name}"""
#         try:
#             ret = False
#             cur = conn.cursor()

#             sql = f"""CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}"
#               (
#               RAW_JSON VARIANT,
#               DSS_START_INTERVAL datetime,
#               DSS_END_INTERVAL datetime
#               );
#               """

#             cur.execute(sql)
#             ret = True
#         except Exception as e:
#             print(
#                 self.script_name,
#                 f"Failed to create table {schema_name}.{table_name}, SQL: {sql}",
#                 e,
#             )
#             ret = False
#             raise e
#         return ret

#     def write_to_snowflake(self, df, interval):
#         """Writing to snowflake"""
#         try:
#             df.columns = ["RAW_JSON"]
#             df["DSS_START_INTERVAL"] = interval["start_interval"]
#             df["DSS_END_INTERVAL"] = interval["end_interval"]

#             df.columns = df.columns.str.upper()
#             for col in df.select_dtypes(include=["datetime64"]).columns.tolist():
#                 df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

#             pkb = self.authorize()
#             conn = self.create_snowflake_connection(pkb)

#             if conn is None:
#                 raise Exception("failed snowflake connection")

#             self.create_table(conn, "STAGE", self.table)

#             if len(df.index) != 0:
#                 write_pandas(
#                     conn=conn, df=df, table_name=self.table, quote_identifiers=True
#                 )
#             else:
#                 raise Exception("empty dataframe")
#         except Exception as e:
#             print("-2")
#             print(self.script_name, "Error writing to snowflake: ", e)
#             raise e

#     def create_red_connection(self):
#         """Connect to the Wherescape Red Database."""
#         conn = None
#         try:
#             conn = pyodbc.connect("DSN=$PREPO_Database$", autocommit=True)
#             return conn
#         except Error as e:
#             print(
#                 self.script_name,
#                 """failed at the create_connection
#                   function with this error:""",
#                 e,
#             )
#         return conn

#     def update_parameter(self, parameter_name, date, description):
#         """Create the parameter update statement."""
#         try:
#             parameter_update = (
#                 """EXEC WsParameterWrite
#               '"""
#                 + parameter_name
#                 + """', '"""
#                 + date
#                 + """',
#               '"""
#                 + description
#                 + """'"""
#             )
#             return parameter_update
#         except Exception as e:
#             print(-2)
#             print(
#                 self.script_name,
#                 """failed at the update_parameter
#                   function with this error:""",
#                 e,
#             )

#     def execute_statement(self, conn, sql):
#         """Execute query in Wherescape Red Database."""
#         try:
#             cur = conn.cursor()
#             cur.execute(sql)
#         except Error as e:
#             print(-2)
#             print(
#                 self.script_name,
#                 """failed at the execute_statement
#                   function with this error:""",
#                 e,
#             )

#     def api_check_request(self, response):
#         """Make api request."""
#         try:
#             if response.status_code < 200 or response.status_code > 299:
#                 raise ResponseError(
#                     f"An api failed to return 200 response: {response.url}"
#                 )
#         except Exception as e:
#             print(-2)
#             print(
#                 self.script_name,
#                 """failed at the api_request_check_function
#                   function with this error:""",
#                 e,
#             )
#             raise e

#     def generate_interval_list(self, start, end):
#         interval_list = []
#         if start < end - relativedelta(months=1):
#             while start + relativedelta(months=1) < end:
#                 start_interval = start.isoformat()
#                 end_interval = (start + relativedelta(months=1)).isoformat()
#                 interval_list.append(
#                     dict(start_interval=start_interval, end_interval=end_interval)
#                 )
#                 start += relativedelta(months=1)
#             start_interval = start.isoformat()
#             end_interval = end.isoformat()
#             interval_list.append(
#                 dict(start_interval=start_interval, end_interval=end_interval)
#             )
#         else:
#             if start < end - relativedelta(days=5):
#                 start_interval = start.isoformat()
#                 end_interval = end.isoformat()
#                 interval_list.append(
#                     dict(start_interval=start_interval, end_interval=end_interval)
#                 )
#             else:
#                 start_interval = (start - relativedelta(days=5)).isoformat()
#                 end_interval = end.isoformat()
#                 interval_list.append(
#                     dict(start_interval=start_interval, end_interval=end_interval)
#                 )
#         return interval_list

#     def get_token_api(self):
#         try:
#             CLIENT_SECRET = SecretServer().get_password(1457, "password").strip()
#             CLIENT_ID = "1e7d0924-6f56-4634-bd35-7b7a5a43bdfe"

#             # Base64 encode the client ID and client secret
#             authorization = base64.b64encode(
#                 bytes(CLIENT_ID + ":" + CLIENT_SECRET, "ISO-8859-1")
#             ).decode("ascii")

#             url = "https://login.usw2.pure.cloud/oauth/token"
#             payload = "grant_type=client_credentials"

#             headers = {
#                 "Authorization": f"Basic {authorization}",
#                 "Content-Type": "application/x-www-form-urlencoded",
#             }

#             response = requests.request(
#                 "POST", url, headers=headers, data=payload, timeout=600
#             )
#             self.api_check_request(response)
#             token = response.json()["access_token"]
#             return token
#         except Exception as e:
#             print(-2)
#             print(self.script_name, """failed to get token""", e)
#             raise e

#     def create_job_api(self, token, interval):
#         try:
#             job_url = "https://api.usw2.pure.cloud/api/v2/analytics/conversations/details/jobs"
#             job_payload = json.dumps(
#                 {
#                     "interval": interval["start_interval"]
#                     + "/"
#                     + interval["end_interval"]
#                 }
#             )

#             job_headers = {
#                 "Authorization": "Bearer " + token,
#                 "Content-Type": "application/json",
#             }
#             job_response = requests.request(
#                 "POST", job_url, headers=job_headers, data=job_payload, timeout=600
#             )
#             self.api_check_request(job_response)

#             jobId = job_response.json()["jobId"]
#             return jobId
#         except Exception as e:
#             print(-2)
#             print(self.script_name, """failed to create job""", e)
#             raise e

#     def check_job_completed(self, token, jobId):
#         try:
#             job_check_url = (
#                 "https://api.usw2.pure.cloud/api/v2/analytics/conversations/details/jobs/"
#                 + jobId
#             )

#             job_check_headers = {"Authorization": "Bearer " + token}

#             state = "not_started"
#             # not fullfilled or out of time.

#             time.sleep(60)

#             end = datetime.datetime.now() + datetime.timedelta(minutes=60)
#             while datetime.datetime.now() < end:
#                 job_check_response = requests.request(
#                     "GET", job_check_url, headers=job_check_headers, timeout=600
#                 )
#                 self.api_check_request(job_check_response)
#                 state = job_check_response.json()["state"]
#                 if state == "FULFILLED":
#                     end = datetime.datetime.now()
#                 elif state in ("FAILED", "CANCELLED", "EXPIRED"):
#                     raise ResponseError(
#                         f"Scheduled job returned an unuseable status {job_check_url}"
#                     )
#                 else:
#                     time.sleep(60)
#         except Exception as e:
#             print(-2)
#             print(self.script_name, """failed on checking for job completion""", e)
#             raise e

#     def get_and_write_results(self, token, jobId, interval):
#         try:
#             row_count = 0
#             params = dict(pageSize=2000)

#             results_url = (
#                 "https://api.usw2.pure.cloud/api/v2/analytics/conversations/details/jobs/"
#                 + jobId
#                 + "/results"
#             )

#             results_headers = {"Authorization": "Bearer " + token}

#             results_response = requests.request(
#                 "GET", results_url, headers=results_headers, params=params, timeout=600
#             )
#             self.api_check_request(results_response)
#             job_results = results_response.json()

#             data = job_results["conversations"]

#             json_list = []
#             for item in data:
#                 json_string = str(item)
#                 json_list.append(json_string)

#             cursor = job_results.get("cursor", "no_more_results")

#             df = pd.DataFrame(json_list)
#             while cursor != "no_more_results":
#                 time.sleep(1)
#                 params = dict(pageSize=1000, cursor=cursor)
#                 paging_response = requests.request(
#                     "GET",
#                     results_url,
#                     headers=results_headers,
#                     params=params,
#                     timeout=600,
#                 )
#                 self.api_check_request(paging_response)
#                 paging_results = paging_response.json()
#                 paging_data = paging_results["conversations"]

#                 paging_json_list = []
#                 for item in paging_data:
#                     paging_json_string = str(item)
#                     paging_json_list.append(paging_json_string)

#                 if len(df.index) != 0:
#                     df = df.append(paging_json_list, ignore_index=True)
#                 else:
#                     df = pd.DataFrame(paging_json_list)

#                 cursor = paging_results.get("cursor", "no_more_results")
#                 if len(df.index) >= 50000:
#                     self.write_to_snowflake(df, interval)
#                     row_count = row_count + len(df.index)
#                     df = df.drop(df.index)

#             self.write_to_snowflake(df, interval)
#             row_count = row_count + len(df.index)
#             return row_count
#         except Exception as e:
#             print(-2)
#             print(self.script_name, """failed on getting and writing results""", e)
#             raise e

#     def main(self):
#         try:
#             start = self.start
#             end = self.end
#             interval_list = self.generate_interval_list(start, end)

#             conn = self.create_red_connection()

#             token = self.get_token_api()

#             for interval in interval_list:
#                 jobId = self.create_job_api(token, interval)

#                 self.check_job_completed(token, jobId)

#                 rows_written = self.get_and_write_results(token, jobId, interval)

#                 update_parameter_test = self.update_parameter(
#                     "Load_Gen_Conv_Start_Interval",
#                     interval["end_interval"],
#                     "Genesys_Conversation Start Interval",
#                 )

#                 self.execute_statement(conn, update_parameter_test)

#             print("1")
#             print("Successfully loaded", rows_written, "rows")
#         except Exception as e:
#             print(-2)
#             print(
#                 self.script_name,
#                 """failed at the main
#                 function with this error:""",
#                 e,
#             )


# if __name__ == "__main__":
#     load = geneysys_load()
#     load.main()
