import json
import random
from datetime import datetime
from random import randint
from time import sleep

import numpy as np
from kafka import KafkaProducer

from util import rand_lat_long, generate_random_point

producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

if __name__ == "__main__":
    num_of_cards = 3
    num_of_users = 3
    card_to_user_map = {
        card_id: random.randint(0, num_of_users - 1)
        for card_id in range(0, num_of_cards)
    }
    user_to_last_location_map = {
        user_id: rand_lat_long() for user_id in range(0, num_of_users)
    }
    card_to_limit_map = {
        card_id: np.abs(np.random.normal(20000, 40000))
        for card_id in range(0, num_of_cards)
    }

    while True:
        card_id = random.randint(0, num_of_cards - 1)
        card_limit = card_to_limit_map[card_id]
        user_id = card_to_user_map[card_id]
        user_location = user_to_last_location_map[user_id]
        new_location = generate_random_point(user_location[0], user_location[1], 100)
        message = {
            "card_id": card_id,
            "user_id": user_id,
            "latitude": new_location[0],
            "longitude": new_location[1],
            "value": np.abs(np.random.normal(card_limit, card_limit / 2)),
            "limit": card_limit,
            "timestamp": str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        }
        producer.send("Transaction", value=message)
        print(f"Transaction | Details = {str(message)}")
        sleep(randint(1, 5))
