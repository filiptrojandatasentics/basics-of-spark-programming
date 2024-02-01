import re


# The user's name (email address) will be used to create a home directory into which all datasets will
# be written into so as to further isolating changes from other students running the same material.
username = "filip.trojan@datasentics.com"
clean_username = re.sub("[^a-zA-Z0-9]", "_", username)

course_name = "developer-foundations-capstone"
clean_course_name = re.sub("[^a-zA-Z0-9]", "_", course_name)

# The path to our user's working directory. This combines both the
# username and course_name to create a "globally unique" folder
working_dir = f"dbfs:/dbacademy/{username}/{course_name}"
meta_dir = f"{working_dir}/raw/orders/batch/2017.txt"

batch_2017_path = f"{working_dir}/raw/orders/batch/2017.txt"
batch_2018_path = f"{working_dir}/raw/orders/batch/2018.csv"
batch_2019_path = f"{working_dir}/raw/orders/batch/2019.csv"
batch_target_path = f"{working_dir}/batch_orders_dirty.delta"

stream_path =                        f"{working_dir}/raw/orders/stream"
orders_checkpoint_path =             f"{working_dir}/checkpoint/orders"
line_items_checkpoint_path =         f"{working_dir}/checkpoint/line_items"

products_xsd_path = f"{working_dir}/raw/products/products.xsd"
products_xml_path = f"{working_dir}/raw/products/products.xml"

batch_source_path = batch_target_path
batch_temp_view = "batched_orders"

user_db = f"""dbacademy_{clean_username}_{clean_course_name}"""

orders_table = "orders"
sales_reps_table = "sales_reps"
products_table = "products"
#product_line_items_table = "product_line_items"
line_items_table = "line_items"

print(batch_2017_path)