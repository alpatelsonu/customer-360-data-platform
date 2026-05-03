import random
from datetime import datetime, timedelta
import csv

IDENTITY_CHANGE_RATIO = 0.15

# load users
user_ids = []
with open("./dataset/raw/customers.csv", "r") as f:
    next(f)
    for line in f:
        user_ids.append(line.strip())

num_changes = int(len(user_ids) * IDENTITY_CHANGE_RATIO)
selected_users = random.sample(user_ids, num_changes)

identity_map = []

def random_effective_date():
    start = datetime(2024, 1, 3)
    end = datetime(2024, 1, 8)
    delta = end - start
    return (start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))).isoformat()

for u in selected_users:
    new_id = f"{u}_new"

    identity_map.append({
        "old_user_id": u,
        "new_user_id": new_id,
        "effective_from": random_effective_date()
    })

# write CSV
with open("./dataset/processed/swap_events.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["old_user_id","new_user_id","effective_from"])
    writer.writeheader()
    writer.writerows(identity_map)