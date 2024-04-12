from argparse import ArgumentParser

import boto3
from botocore.config import Config

import pandas as ps
import json
import csv


class CognitoExporter(object):
    """Cognito client, user info un-roller and exporter"""

    def __init__(self, user_pool_id: str, region: str, output_path: str) -> None:
        cognito_config = Config(
            region_name=region, retries={"max_attempts": 10, "mode": "adaptive"}
        )

        self.client = boto3.client("cognito-idp")

        self.user_pool_id = user_pool_id
        self.region = region
        self.output_path = output_path

        self.write_dict = {}

        self.csv_headers = self.get_csv_headers()
        # self.jsonlike_csv_headers = [
        #     csv_header.title().replace(" ", "") for csv_header in self.csv_headers
        # ]

        ###Sets up dict to write with csv headers
        for header in self.csv_headers:
            self.write_dict[header] = []

        return

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

        print(user)

        # for key in self.write_dict.keys():
        #     jsonlike_key = key.replace("_", " ").title().replace(" ", "")

        #     if jsonlike_key not in user.keys():
        #         if key in [
        #             attribute_dict["Name"] for attribute_dict in user["Attributes"]
        #         ]:
        #             self.write_dict[key].append(
        #                 attribute_dict["Value"]
        #                 for attribute_dict in user["Attributes"]
        #                 if attribute_dict["Name"] == key
        #             )
        #     else:
        #         self.write_dict[key] = user[jsonlike_key]

        return

    def populate_dict(self):
        """Gets response batches of user data to then write individually"""

        pagination_token = ""

        while pagination_token != None:

            user_response = self.client.list_users(UserPoolId=self.user_pool_id)

            users = user_response["Users"]

            for user in users:
                self.unwrap_and_store_user(user)

            if pagination_token in user_response.keys():
                pagination_token = user_response["PaginationToken"]
            else:
                pagination_token = None

        return

    def export(self):
        """Exports dict of unrolled user data to csv"""

        df = ps.DataFrame.from_dict(self.write_dict)

        df.to_csv(self.output_path, index=False)


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
        "--output-path",
        dest="output_path",
        help="output csv path (must end in .csv)",
        required=True,
        type=str,
    )

    args = parser.parse_args()

    keys_csv_path = args.access_keys_csv_path

    aws_access_key_id, aws_secret_access_key, aws_session_token = (
        get_access_keys_from_csv(keys_csv_path)
    )

    exporter = CognitoExporter(args.user_pool_id, args.region, args.output_path)

    exporter.populate_dict()

    exporter.export()
