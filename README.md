## aws-mwaa-docker-images

## Overview

This repository contains the Docker Images that [Amazon MWAA](https://aws.amazon.com/managed-workflows-for-apache-airflow/)
will use in future versions of Airflow. Eventually, we will deprecate [aws-mwaa-local-runner](https://github.com/aws/aws-mwaa-local-runner)
in favour of this package. However, at this point, this repository is still under development.

## Using the Airflow Image

Currently, Airflow v2.9.0 is supported. Future versions in parity with Amazon MWAA will be added.

To experiment with the image using a vanilla Docker setup, follow these steps:

0. Ensure you have:
    - Python 3.11 or later.
    - Docker and Docker Compose.
1. Clone this repository.
2. This repository makes use of Python virtual environments. To create them, from the root of the package, execute the following command:
```
python3 create_venvs.py
```
3. Build the Airflow v2.9.0 Docker image using:
```
cd <amazon-mwaa-docker-images path>/images/airflow/2.9.0 
./run.sh
```

Airflow should be up and running now. You can access the web server on your localhost on port 8080.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
