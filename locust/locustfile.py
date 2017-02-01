from locust import HttpLocust, TaskSet, task
import random
import string

chars = string.ascii_lowercase + string.ascii_uppercase + string.digits
def random_word(size):
  word = ""
  for i in range(0, size):
      index = random.randint(0, len(chars)-1)
      word += chars[index]
  return word


class UserBehaviour(TaskSet):
    @task(20)
    def send_random_event(self):
        self.client.post("/event", json={
            "username": random_word(8),
            "count": random.randint(0, 99999999),
            "metric": random_word(4),
        })

    @task(1)
    def send_example_event(self):
        self.client.post("/event", json={
            "username": "kodingbot",
            "count": 12412414,
            "metric": "kite_call",
        })

class User(HttpLocust):
    task_set = UserBehaviour
