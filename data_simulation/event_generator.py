import json
import random
import csv
from datetime import datetime, timedelta

NUM_EVENTS = 20000
EVENT_TYPES = ["view", "click", "add_to_cart", "purchase"]

# Load users
user_ids = []
with open("./dataset/raw/customers.csv", "r") as f:
    next(f)
    for line in f:
        user_ids.append(line.strip())

# Load identity map
identity_map = {}

with open("./dataset/processed/identity_map.csv", "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        identity_map[row["old_user_id"]] = {
            "new_user_id": row["new_user_id"],
            "effective_from": datetime.fromisoformat(row["effective_from"])
        }

# Timestamp generator
def random_timestamp():
    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 10)
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

# Generate events
events = []

for i in range(NUM_EVENTS):
    base_user = random.choice(user_ids)
    event_time = random_timestamp()

    # Identity logic
    if base_user in identity_map:
        mapping = identity_map[base_user]
        if event_time >= mapping["effective_from"]:
            user_id = mapping["new_user_id"]
        else:
            user_id = base_user
    else:
        user_id = base_user

    event = {
        "event_id": f"e_{i}",
        "event_type": random.choice(EVENT_TYPES),
        "user_id": user_id,
        "device_id": f"d_{random.randint(1,300)}",
        "session_id": f"s_{random.randint(1,5000)}",
        "product_id": f"p_{random.randint(1,100)}",
        "event_timestamp": event_time.isoformat()
    }

    # Introduce duplicates (5%)
    if random.random() < 0.05:
        events.append(event)

    events.append(event)

# Write JSON (newline-delimited)
with open("./dataset/processed/user_events.json", "w") as f:
    for e in events:
        f.write(json.dumps(e) + "\n")