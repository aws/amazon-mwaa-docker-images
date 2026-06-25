#!/usr/bin/env python3
"""
This Module generates Fernet keys, which are used by Airflow for connection encryption
"""

from cryptography.fernet import Fernet
import json

def generate_fernet_key():
    """
    Generate a Fernet key and return it as a JSON string.

    :returns A JSON string containing the generated Fernet key in the format {"FernetKey": "<key>"}
    """
    key = Fernet.generate_key().decode()
    return json.dumps({"FernetKey": key})

if __name__ == "__main__":
    print(generate_fernet_key())
