import os
import json
import boto3

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

from botocore.exceptions import ClientError


class Config:
    def __init__(self):
        self.secrets_manager = boto3.client('secretsmanager', region_name='us-east-1')

    def get_secret_from_aws(self, secret_name):
        try:
            response = self.secrets_manager.get_secret_value(SecretId=secret_name)
            if 'SecretString' in response:
                return json.loads(response['SecretString'])
            return None
        except (ClientError,):
            return None

    def get_config_value(self, key, default_value, secret_name=None):
        if secret_name:
            aws_secret = self.get_secret_from_aws(secret_name)
            if aws_secret and key in aws_secret:
                return aws_secret
            if key in os.environ:
                return os.environ[key]
            return default_value

    def get_db_config(self):
        return {
            "name": self.get_config_value("DB_NAME", "postgres", "python_interview_dbname"),
            "user": self.get_config_value("DB_USER", "postgres", "python_interview_user"),
            "password": self.get_config_value("DB_PASSWORD", "password", "python_interview_password"),
            "host": self.get_config_value("DB_HOST", "localhost", "python_interview_host"),
            "port": self.get_config_value("DB_PORT", "5432", "python_interview_port")
        }

    def get_conn_string(self):
        db_config = self.get_db_config()
        return "postgresql://{user}:{password}@{host}:{port}/{name}".format(
            name=db_config['name'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port']
        )


class DatabaseConnection:
    def __init__(self, conn_string):
        self.engine = create_engine(conn_string)
        self.session = sessionmaker(bind=self.engine)

    def get_session(self):
        new_session = self.session()
        return new_session


def dedupe_data(session, table_name):
    records = session.execute(text(f"SELECT * FROM {table_name}")).fetchall()

    duplicates = []
    outliers = []
    for record in records:
        print(record)
        # Insert your anomaly detection logic here


if __name__ == "__main__":
    config = Config()
    conn_string = config.get_conn_string()
    db = DatabaseConnection(conn_string)
    session = db.get_session()

    dedupe_data(session, "crypto_transactions")
