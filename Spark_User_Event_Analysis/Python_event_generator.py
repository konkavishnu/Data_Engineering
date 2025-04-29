from confluent_kafka import Producer
import json
import random
import time
from faker import Faker

# Initialize Faker for data generation
fake = Faker()

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'  # Update with your Kafka broker address
USER_CLICK_TOPIC = 'user_click'
ORDER_DETAILS_TOPIC = 'order_details_topic'

# Kafka Producer Initialization
producer = Producer({'bootstrap.servers': KAFKA_BROKER})


# Function to simulate user click event
def produce_user_click_event(event_count):
    for _ in range(event_count):
        event_data = {
            "event_type": "user_click",
            "user_id": random.randint(1, 1000),
            "timestamp": time.time(),
            "page": fake.uri_path(),
            "device": random.choice(["Desktop", "Mobile", "Tablet"]),
            "location": fake.city()
        }
        print(f"Producing User Click Event: {json.dumps(event_data, indent=2)}")
        producer.produce(USER_CLICK_TOPIC, json.dumps(event_data))
    producer.flush()
    print(f"{event_count} User Click Events Produced Successfully!")


# Function to simulate order placed event
def produce_order_placed_event(event_count):
    for _ in range(event_count):
        order_data = {
            "event_type": "order_placed",
            "order_id": fake.uuid4(),
            "user_id": random.randint(1, 1000),
            "order_amount": round(random.uniform(10, 5000), 2),
            "transaction_id": fake.uuid4(),
            "payment_method": random.choice(["Credit Card", "Debit Card", "UPI", "Net Banking"]),
            "timestamp": time.time(),
            "items": [
                {
                    "item_id": random.randint(1, 10000),  # Updated to range 1 - 10000
                    "item_name": fake.word(),
                    "price": round(random.uniform(5, 100), 2)
                }
                for _ in range(random.randint(1, 5))
            ]
        }
        print(f"Producing Order Placed Event: {json.dumps(order_data, indent=2)}")
        producer.produce(ORDER_DETAILS_TOPIC, json.dumps(order_data))
    producer.flush()
    print(f"{event_count} Order Placed Events Produced Successfully!")


# Main Function to Trigger Events
def main():
    print("Event Producer for Kafka Started...")
    print("Available Events:")
    print("1. User Click Event")
    print("2. Order Placed Event")
    print("3. Exit")

    while True:
        choice = input("\nEnter the number of the event to trigger (1/2/3): ").strip()

        if choice in ["1", "2"]:
            try:
                event_count = int(input("How many events do you want to generate? ").strip())
                if event_count <= 0:
                    print("Please enter a positive number!")
                    continue
            except ValueError:
                print("Invalid input! Please enter a valid number.")
                continue

            if choice == "1":
                produce_user_click_event(event_count)
            elif choice == "2":
                produce_order_placed_event(event_count)
        elif choice == "3":
            print("Exiting...")
            break
        else:
            print("Invalid choice! Please enter 1, 2, or 3.")


if __name__ == "__main__":
    main()
