# cur.execute(
#     """
#     CREATE TABLE IF NOT EXISTS {} datacamp_courses(
#         id SERIAL PRIMARY KEY,
#         tpep_pickup_datetime TIMESTAMP,
#         tpep_dropoff_datetime TIMESTAMP,
#         total_amount FLOAT,
#         processed_time TIMESTAMP
#     );
#     """.format(
#         sql.Identifier(table_name)
#     )
# )
