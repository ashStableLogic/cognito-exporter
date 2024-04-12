import pandas as ps
import json


def handle_user(user):
    global write_json
    pass


if __name__ == "__main__":

    users = []
    write_json = {}

    with open("./cognito_dummy_pool.json", encoding="utf-16") as read_file:
        users = json.load(read_file)["Users"]

    users = users[:50]  ##Truncating for testing

    for user in users:
        handle_user(user)
