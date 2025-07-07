from confluent_kafka import Producer
import json
import random
import time
from faker import Faker

# Initialize Faker for generating fake data
fake = Faker()

# Kafka Configuration
KAFKA_BROKER = '' # Update with your Kafka broker address
USER_CLICK_TOPIC = 'user_click'
ORDER_DETAILS_TOPIC = 'order_details_topic'

# Kafka Producer Setup
producer = Producer({'bootstrap.servers': KAFKA_BROKER})


def produce_user_click_event(event_count):
    for _ in range(event_count):
        event_data = {
            "event_type": "user_click",
            "user_id": random.randint(1, 1000),
            "timestamp": int(time.time() * 1000),  # epoch in ms
            "page": fake.uri_path(),
            "device": random.choice(["Desktop", "Mobile", "Tablet"]),
            "location": fake.city()
        }
        print(f"Producing User Click Event:\n{json.dumps(event_data, indent=2)}")
        producer.produce(USER_CLICK_TOPIC, json.dumps(event_data))

    producer.flush()
    print(f"{event_count} User Click Events Produced Successfully!\n")


def produce_order_placed_event(event_count):
    for _ in range(event_count):
        order_data = {
            "event_type": "order_placed",
            "order_id": fake.uuid4(),
            "user_id": random.randint(1, 1000),
            "order_amount": round(random.uniform(10, 5000), 2),
            "transaction_id": fake.uuid4(),
            "payment_method": random.choice(["Credit Card", "Debit Card", "UPI", "Net Banking"]),
            "timestamp": int(time.time() * 1000),
            "items": [
                {
                    "item_id": random.randint(1, 10000),
                    "item_name": fake.word(),
                    "price": round(random.uniform(5, 100), 2)
                }
                for _ in range(random.randint(1, 5)) #one order can contain 1 to multiple items, it will generate this.
            ]
        }
        print(f"Producing Order Placed Event:\n{json.dumps(order_data, indent=2)}")
        producer.produce(ORDER_DETAILS_TOPIC, json.dumps(order_data))

    producer.flush()
    print(f"{event_count} Order Placed Events Produced Successfully!\n")


def main():
    print("Kafka Event Producer Started...")
    while True:
        print("\n1. Produce User Click Events")
        print("2. Produce Order Placed Events")
        print("3. Exit")
        choice = input("Enter your choice: ").strip()

        if choice in {"1", "2"}:
            try:
                event_count = int(input("How many events? ").strip())
                if event_count <= 0:
                    print("Please enter a positive number.")
                    continue
            except ValueError:
                print("Invalid input. Please enter a valid number.")
                continue

            if choice == "1":
                produce_user_click_event(event_count)
            elif choice == "2":
                produce_order_placed_event(event_count)

        elif choice == "3":
            print("Exiting producer.")
            break
        else:
            print("Invalid choice. Enter 1, 2, or 3.")


if __name__ == "__main__":
    main()
