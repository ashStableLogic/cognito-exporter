from argparse import ArgumentParser

import boto3
from botocore.config import Config

import pandas as ps
import json
import csv

import re

import os


class CognitoExporter(object):
    """Cognito client, user info un-roller and exporter"""

    def __init__(
        self,
        aws_access_key_id,
        aws_secret_access_key,
        aws_session_token,
        user_pool_id: str,
        region: str,
        output_dir: str,
    ) -> None:

        self.user_pool_id = user_pool_id
        self.region = region
        self.output_dir = output_dir

        self.create_dir_if_none(output_dir)

        cognito_config = Config(
            region_name=region,
            retries={"max_attempts": 10, "mode": "adaptive"},
        )

        self.client = boto3.client(
            "cognito-idp",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            config=cognito_config,
        )

        self.full_output_path = os.path.abspath(
            self.output_dir + "/" + self.get_user_pool_name() + ".csv"
        )

        self.write_dict = {}

        self.csv_headers = self.get_csv_headers()
        # self.jsonlike_csv_headers = [
        #     csv_header.title().replace(" ", "") for csv_header in self.csv_headers
        # ]

        ###Sets up dict to write with csv headers
        for header in self.csv_headers:
            self.write_dict[header] = []

        self.len_headers = len(self.write_dict)

        return

    def create_dir_if_none(self, dir: str) -> None:
        absolute_path = os.path.abspath(dir)

        if not os.path.exists(absolute_path):
            os.makedirs(absolute_path)

        return

    def get_user_pool_name(self) -> str:
        response = self.client.describe_user_pool(UserPoolId=self.user_pool_id)

        return response["UserPool"]["Name"]

    def get_csv_headers(self) -> list:
        """Returns list of csv headers for user pool

        Returns:
            csv_headers (list): List of csv headers for user pool
        """
        response = self.client.get_csv_header(UserPoolId=self.user_pool_id)

        csv_headers = response["CSVHeader"]

        return csv_headers

    def unwrap_and_store_user(self, user) -> None:
        """Unwraps individual user data and stores it in self-contained dict to write later

        Args:
            user (dict): Json for individual user
        """

        PREFIXES = ["", "cognito:", "cognito:mfa_", "custom:"]

        blank_record = dict.fromkeys(self.csv_headers, "")

        ### PLAN
        ##  MAKE BLANK DICT RECORD
        ##  GO THROUGH KEYS
        ##  MAKE JSONLIKE STRING FOR EACH KEY
        ##  CHECK JSONLIKE STRING AGAIN EACH KEY IN USER

        ###Converts key-value structure from Name:'key name', Value:'value name' to more traditional one
        user["Attributes"] = {
            attribute["Name"]: attribute["Value"] for attribute in user["Attributes"]
        }

        for user_key in user.keys():
            if user_key == "Attributes":
                attributes = user[user_key]

                for attribute_key in attributes.keys():
                    if attribute_key in blank_record.keys():
                        blank_record[attribute_key] = attributes[attribute_key]

            else:
                json_key_upper_words = re.findall("[A-Z][^A-Z]*", user_key)

                ##Makes key look like cvs headers
                json_key_converted = "_".join(json_key_upper_words).lower()

                for PREFIX in PREFIXES:
                    if PREFIX + json_key_converted in blank_record.keys():
                        blank_record[PREFIX + json_key_converted] = user[user_key]

        # for key in blank_record.keys():
        #     if ":" in key:
        #         split_key = key.split(":")[1]

        #     jsonlike_key = key.replace("_", " ").title().replace(" ", "")

        #     if jsonlike_key in user.keys():
        #         blank_record[key] = user[jsonlike_key]
        #     elif key in user["Attributes"].keys():
        #         blank_record[key] = user["Attributes"][key]

        [self.write_dict[key].append(blank_record[key]) for key in self.write_dict]

        return

    def populate_dict(self):
        """Gets response batches of user data to then write individually"""

        pagination_token = ""

        user_response = self.client.list_users(UserPoolId=self.user_pool_id)

        users = user_response["Users"]

        for user in users:
            self.unwrap_and_store_user(user)

        if "PaginationToken" in user_response.keys():
            pagination_token = user_response["PaginationToken"]
        else:
            pagination_token = None

        while pagination_token != None:
            user_response = self.client.list_users(
                UserPoolId=self.user_pool_id, PaginationToken=pagination_token
            )

            users = user_response["Users"]

            for user in users:
                self.unwrap_and_store_user(user)

            if "PaginationToken" in user_response.keys():
                pagination_token = user_response["PaginationToken"]
            else:
                pagination_token = None

        return

    def export(self):
        """Exports dict of unrolled user data to csv"""

        df = ps.DataFrame.from_dict(self.write_dict)

        df.to_csv(self.full_output_path, index=False)


def get_access_keys_from_csv(keys_csv_path: str) -> tuple[str, str, str]:
    aws_access_key_id = ""
    aws_secret_access_key = ""
    aws_session_token = ""

    with open(keys_csv_path, "r") as keys_file:
        reader = csv.reader(keys_file)
        keys_data = list(reader)[1]

    if len(keys_data) == 2:
        aws_access_key_id = keys_data[0]
        aws_secret_access_key = keys_data[1]
    elif len(keys_data) == 3:
        aws_session_token = keys_data[2]

    return aws_access_key_id, aws_secret_access_key, aws_session_token


if __name__ == "__main__":

    write_dict = {}

    # Args are in the form:
    # this_program -k path_to_csv.csv -r eu-west-1 us-east-1 us-east-2 etc.

    parser = ArgumentParser()

    parser.add_argument(
        "-k",
        "--access-keys-csv-path",
        dest="access_keys_csv_path",
        help="Access keys CSV path",
        required=True,
        type=str,
    )

    parser.add_argument(
        "-r",
        "--region",
        dest="region",
        help="Region name of user pool",
        required=True,
        type=str,
    )

    parser.add_argument(
        "-id",
        "--user-pool-id",
        dest="user_pool_id",
        help="ID string for user pool of interest",
        required=True,
        type=str,
    )

    parser.add_argument(
        "-o",
        "--output-dir",
        dest="output_dir",
        help="output directory path",
        required=True,
        type=str,
    )

    args = parser.parse_args()

    keys_csv_path = args.access_keys_csv_path

    user_pool_id = args.user_pool_id

    region = args.region

    output_dir = args.output_dir

    aws_access_key_id, aws_secret_access_key, aws_session_token = (
        get_access_keys_from_csv(keys_csv_path)
    )

    exporter = CognitoExporter(
        aws_access_key_id,
        aws_secret_access_key,
        aws_session_token,
        user_pool_id,
        region,
        output_dir,
    )

    exporter.populate_dict()

    exporter.export()
