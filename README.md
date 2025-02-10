## aws-mwaa-docker-images

## Overview

This repository contains the Docker Images that [Amazon
MWAA](https://aws.amazon.com/managed-workflows-for-apache-airflow/) uses to run Airflow.

You can also use it locally if you want to run a MWAA-like environment for testing, experimentation,
and development purposes.

Currently, Airflow v2.9.2 and above are supported. Future versions in parity with Amazon MWAA will be added as
well. _Notice, however, that we do not plan to support previous Airflow versions supported by MWAA._

## Using the Airflow Image

To experiment with the image using a vanilla Docker setup, follow these steps:

0. _(Prerequisites)_ Ensure you have:
   - Python 3.11 or later.
   - [Docker](https://docs.docker.com/desktop/) and [Docker Compose](https://docs.docker.com/compose/install/)
1. Clone this repository.
2. This repository makes use of Python virtual environments. To create them, from the root of the
   package, execute the following command:

```
python3 create_venvs.py --target <development | production>
```

3. Build a supported Airflow version Docker image
   - `cd <amazon-mwaa-docker-images path>/images/airflow/2.9.2`
   - Update `run.sh` file with your account ID, environment name and account credentials. The permissions associated
   with the provided credentials will be assigned to the Airflow components that would be started with the next step. 
   So, if you receive any error message indicating lack of permissions, then try providing the permissions to the 
   identity whose credentials were used.
   - Create the required log groups in the dev account with the names:
     - `{ENV_NAME}-DAGProcessing`
     - `{ENV_NAME}-Scheduler`
     - `{ENV_NAME}-Worker`
     - `{ENV_NAME}-Task`
     - `{ENV_NAME}-WebServer`
   - `./run.sh` This will build and run all the necessary containers.

Airflow should be up and running now. You can access the web server on your localhost on port 8080.

### Generated Docker Images

When you build the Docker images of a certain Airflow version, using either `build.sh` or `run.sh`
(which automatically also calls `build.sh` for you), multiple Docker images will actually be
generated. For example, for Airflow 2.9, you will notice the following images:

| Repository                        | Tag                           |
| --------------------------------- | ----------------------------- |
| amazon-mwaa-docker-images/airflow | 2.9.2                         |
| amazon-mwaa-docker-images/airflow | 2.9.2-dev                     |
| amazon-mwaa-docker-images/airflow | 2.9.2-explorer                |
| amazon-mwaa-docker-images/airflow | 2.9.2-explorer-dev            |
| amazon-mwaa-docker-images/airflow | 2.9.2-explorer-privileged     |
| amazon-mwaa-docker-images/airflow | 2.9.2-explorer-privileged-dev |

Each of the postfixes added to the image tag represents a certain build type, as explained below:

- `explorer`: The 'explorer' build type is almost identical to the default build type except that it
  doesn't include an entrypoint, meaning that if you run this image locally, it will not actually
  start Airflow. This is useful for debugging purposes to run the image and look around its content
  without starting airflow. For example, you might want to explore the file system and see what is
  available where.
- `privileged`: Privileged images are the same as their non-privileged counterpart except that they
  run as the `root` user instead. This gives the user of this Docker image
  elevated permissions. This can be useful if the user wants to do some experiments as the root
  user, e.g. installing DNF packages, creating new folders outside the airflow user folder, among
  others.
- `dev`: These images have extra packages installed for debugging purposes. For example, typically
  you wouldn't want to install a text editor in a Docker image that you use for production. However,
  during debugging, you might want to open some files and inspect their contents, make some changes,
  etc. Thus, we install an editor in the dev images to aid with such use cases. Similarly, we
  install tools like `wget` to make it possible for the user to fetch web pages. For a complete
  listing of what is installed in `dev` images, see the `bootstrap-dev` folders.


## Extra commands

#### Requirements

- Add Python dependencies to `requirements/requirements.txt`
- To test a `requirements.txt` without running Apache Airflow, run:
```bash
./run.sh test-requirements
```

#### Startup script

- There is a folder in each airflow version called `startup_script`. Add your script there as `startup.sh`
- If there is a need to run additional setup (e.g. install system libraries, setting up environment variables), please modify the `startup.sh` script.
- To test a `startup.sh` without running Apache Airflow, run:
```bash
./run.sh  test-startup-script
```

#### Reset database

- If you encountered [the following error](https://issues.apache.org/jira/browse/AIRFLOW-3678): `process fails with "dag_stats_table already exists"`, you'll need to reset your database. You just need to restart your container by exiting and rerunning the `run.sh` script

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
