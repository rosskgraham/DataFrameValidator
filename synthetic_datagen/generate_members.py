"""
Synthetic member data generation using Faker
https://faker.readthedocs.io/en/master/index.html
"""

import polars as pl
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()


def generate_ni_number():
    """
    Generate fake UK National Insurance Number
    """
    prefix = ""
    while prefix in ["", "GB", "NK", "TN", "ZZ", "NT", "KN", "BG"]:
        prefix = random.choice(
            [c for c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ" if c not in "DFIQUV"]
        ) + random.choice(
            [c for c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ" if c not in "DFIOQUV"]
        )
    digits = "".join([str(random.randint(0, 9)) for _ in range(6)])
    suffix = random.choice(["A", "B", "C", "D"])
    return f"{prefix}{digits}{suffix}"


def generate_member(member_id):
    return {
        "Member ID": member_id,
        "Gender": (gender := random.choice(["Male", "Female"])),
        "Title": fake.prefix_male() if gender == "Male" else fake.prefix_female(),
        "Forename": fake.first_name_male()
        if gender == "Male"
        else fake.first_name_female(),
        "Surname": fake.last_name(),
        "Nino": generate_ni_number(),
        "Date Of Birth": (
            dob := fake.date_between(start_date="-110y", end_date="-18y")
        ),
        "Joining Date": (
            djs := fake.date_between(
                start_date=dob.replace(year=dob.year + 18, day=min(dob.day, 28)),
                end_date=min(
                    dob.replace(year=dob.year + 75, day=min(dob.day, 28)),
                    datetime.today().date(),
                ),
            )
        ),
        "Date Of Death": random.choices(
            [fake.date_between(start_date=djs, end_date="today"), None], weights=[1, 9]
        )[0],
        "Status": random.choice(["Active", "Retired", "Deferred"]),
    }


num_members = 1000
members = [generate_member(i) for i in range(1, num_members + 1)]

# Introduce some bad data

# No gender marker
ix = random.randint(0, num_members - 1)
members[ix]["Gender"] = None
# Duplicate Nino
ix = random.randint(0, num_members - 1)
members[ix]["Nino"] = members[random.randint(0, num_members - 1)]["Nino"]
# Invalid Nino
ix = random.randint(0, num_members - 1)
members[ix]["Nino"] = "ABC123"
# Invalid Status
ix = random.randint(0, num_members - 1)
members[ix]["Status"] = "Inactive"
# DOB > Join Date
ix = random.randint(0, num_members - 1)
members[ix]["Date Of Birth"] = members[ix]["Joining Date"] + timedelta(days=1)

df = pl.DataFrame(members)
with pl.Config(tbl_cols=len(df.columns)):
    print(df)

# Save to CSV
df.write_csv("pension_scheme_members.csv")
