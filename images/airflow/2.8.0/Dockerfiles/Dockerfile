#
# WARNING: Don't change this file manually. This file is auto-generated from
# the Jinja2-templated Dockerfile.j2 file, so you need to change that file
# instead.
#
# This file was generated on 2024-01-30 16:34:46.229453
#

FROM public.ecr.aws/amazonlinux/amazonlinux:2023

# Environment variables
ENV AIRFLOW_AMAZON_PROVIDERS_VERSION=8.13.0
ENV AIRFLOW_USER_HOME=/usr/local/airflow
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}
ENV AIRFLOW_VERSION=2.8.0
ENV MWAA_HOME=/usr/local/mwaa
ENV PYTHON_VERSION=3.11.7

# We don't want those variables to stay in the final image, so we use ARG instead of ENV.
ARG PATH_DEFAULT=${PATH}
ARG PATH_AIRFLOW_USER=${AIRFLOW_USER_HOME}/.local/bin:${PATH_DEFAULT}
ARG PYTHON_MD5_CHECKSUM=d96c7e134c35a8c46236f8a0e566b69c
ARG MARIADB_DOWNLOAD_BASE_URL=https://mirror.mariadb.org/yum/11.1/fedora38-amd64/rpms
ARG MARIADB_RPM_COMMON=MariaDB-common-11.1.2-1.fc38.x86_64.rpm
ARG MARIADB_RPM_COMMON_CHECKSUM=e87371d558efa97724f3728fb214cf19
ARG MARIADB_RPM_DEVEL=MariaDB-devel-11.1.2-1.fc38.x86_64.rpm
ARG MARIADB_RPM_DEVEL_CHECKSUM=cfce6e9b53f4e4fb1cb14f1ed720c92c
ARG MARIADB_RPM_SHARED=MariaDB-shared-11.1.2-1.fc38.x86_64.rpm
ARG MARIADB_RPM_SHARED_CHECKSUM=ed82ad5bc5b35cb2719a9471a71c6cdb

# Copy bootstrapping files.
COPY ./bootstrap /bootstrap
RUN chmod -R +x /bootstrap

#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# BEGINNING marker for root user, first pass bootstrapping steps.
#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>


RUN /bootstrap/01-root-firstpass/001-init.sh

RUN /bootstrap/01-root-firstpass/002-install-python.sh

RUN /bootstrap/01-root-firstpass/003-install-mariadb.sh

RUN /bootstrap/01-root-firstpass/004-create-airflow-user.sh

RUN /bootstrap/01-root-firstpass/005-install-aws-cli.sh

RUN /bootstrap/01-root-firstpass/100-install-needed-dnf-packages.sh


#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# END marker for root user, first pass bootstrapping steps.
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<



#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# > BEGINNING marker for airflow user bootstrapping steps.
#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# Switch to 'airflow' user and update the PATH environment variable.
USER airflow
ENV PATH=${PATH_AIRFLOW_USER} 


RUN /bootstrap/02-airflow/001-install-airflow.sh


# Revert the PATH and user.
ENV PATH=${PATH_DEFAULT}
USER root

#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# > END marker for airflow user bootstrapping steps.
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<



#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# BEGINNING marker for root user, second pass bootstrapping steps.
#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# Those steps are also executed as the root user. However, they rely on the
# successfull execution of the airflow user bootstrapping steps. For example,
# giving ownership of the Airflow home user to the 'airflow' user requires the
# the folder to be fully setup first.


RUN /bootstrap/03-root-secondpass/001-create-mwaa-dir.sh

RUN /bootstrap/03-root-secondpass/100-chown-airflow-folder.sh


#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# END marker for root user, second pass bootstrapping steps.
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

# remove bootstrapping files.
RUN rm -rf /bootstrap

# Create a volume for syncing files with the sidecar. The actual folder
# is created by the `001-create-mwaa-dir.sh` script.
VOLUME ["${MWAA_HOME}"]

# TODO We should only expose this port if the command is 'webserver'.
EXPOSE 8080

ENV PATH=${PATH_AIRFLOW_USER}
ENV PYTHONPATH="/python"

WORKDIR ${AIRFLOW_USER_HOME}

# Copy python files.
COPY ./python /python



USER airflow

ENTRYPOINT ["python3", "-m", "mwaa.entrypoint"]

CMD shell
