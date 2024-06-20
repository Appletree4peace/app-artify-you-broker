from pushover import Pushover
import os

pushover = Pushover(os.getenv("PUSHOVER_APP_TOKEN"), os.getenv("PUSHOVER_USER_KEY"))
try:
    pushover.send("4", "message")
except Exception as e:
    print(e)