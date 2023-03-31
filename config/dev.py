import os

SETTINGS = {
    "TASKS_DEV": os.getenv("TASKS_DEV", "").split(",")
}
