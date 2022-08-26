import random
from datetime import datetime
from random import randrange

import pandas as pd
from faker import Faker

nr_of_customers = 100


fake = Faker()

customers = []

for customers_id in range(nr_of_customers):
    for i in range(7):
        # Create transaction date
        d1 = datetime.strptime(f"1/1/2021 09:15:32", "%m/%d/%Y %H:%M:%S")
        d2 = datetime.strptime(f"1/8/2021 09:15:32", "%m/%d/%Y %H:%M:%S")
        transaction_date = fake.date_between(d1, d2)

        # create amount spent
        amount_spent = fake.pyfloat(
            right_digits=2, positive=True, min_value=1000, max_value=100000
        )

        customers.append([customers_id, transaction_date, amount_spent])

customers_df = pd.DataFrame(
    customers,
    columns=[
        "id",
        "Transaction_time",
        "Amount_spent",
    ],
)

pd.pandas.set_option("display.max_columns", None)
customers_df.to_csv("data.csv", index=False)
