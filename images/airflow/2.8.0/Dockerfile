#
# WARNING: Don't change this file manually. This file is auto-generated from the
# Jinja2-templated Dockerfile.j2 file, so you need to change that file instead.
#
# This file was generated on 2024-01-17 21:57:12.451896
#

FROM public.ecr.aws/amazonlinux/amazonlinux:2023

# Versions
ENV AIRFLOW_VERSION=2.8.0
ENV AIRFLOW_AMAZON_PROVIDERS_VERSION=8.13.0
ENV PYTHON_VERSION=3.11
ENV AIRFLOW_USER_HOME=/usr/local/airflow
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

ENV PATH_DEFAULT=${PATH}
ENV PATH_AIRFLOW_USER=${AIRFLOW_USER_HOME}/.local/bin:${PATH_DEFAULT}

# Bootstrapping steps (root user - first pass)

COPY ./bootstrap/01-root-firstpass/001-init.sh /001-init.sh
RUN chmod +x /001-init.sh && /001-init.sh
RUN rm /001-init.sh

COPY ./bootstrap/01-root-firstpass/002-install-python.sh /002-install-python.sh
RUN chmod +x /002-install-python.sh && /002-install-python.sh
RUN rm /002-install-python.sh

COPY ./bootstrap/01-root-firstpass/003-install-mariadb.sh /003-install-mariadb.sh
RUN chmod +x /003-install-mariadb.sh && /003-install-mariadb.sh
RUN rm /003-install-mariadb.sh

COPY ./bootstrap/01-root-firstpass/004-create-airflow-user.sh /004-create-airflow-user.sh
RUN chmod +x /004-create-airflow-user.sh && /004-create-airflow-user.sh
RUN rm /004-create-airflow-user.sh

COPY ./bootstrap/01-root-firstpass/005-install-aws-cli.sh /005-install-aws-cli.sh
RUN chmod +x /005-install-aws-cli.sh && /005-install-aws-cli.sh
RUN rm /005-install-aws-cli.sh

COPY ./bootstrap/01-root-firstpass/999-install-needed-dnf-packages.sh /999-install-needed-dnf-packages.sh
RUN chmod +x /999-install-needed-dnf-packages.sh && /999-install-needed-dnf-packages.sh
RUN rm /999-install-needed-dnf-packages.sh


# Bootstrapping steps (airflow user)

USER root
COPY ./bootstrap/02-airflow/001-install-airflow.sh /001-install-airflow.sh
RUN chmod +x /001-install-airflow.sh
ENV PATH=${PATH_AIRFLOW_USER}}
USER airflow
RUN /001-install-airflow.sh
ENV PATH=${PATH_DEFAULT}
USER root
RUN rm /001-install-airflow.sh


# Bootstrapping steps (root user - second pass)
# Put in these steps stuff that you want to execute as a root user
# and also relies on the successful execution of the bootstrapping
# steps of the 'airflow' user. For example, giving ownership of the
# Airflow home user to the 'airflow' user, which requires having all
# files.

COPY ./bootstrap/03-root-secondpass/001-create-mwaa-dir.sh /001-create-mwaa-dir.sh
RUN chmod +x /001-create-mwaa-dir.sh && /001-create-mwaa-dir.sh
RUN rm /001-create-mwaa-dir.sh

COPY ./bootstrap/03-root-secondpass/999-chown-airflow-folder.sh /999-chown-airflow-folder.sh
RUN chmod +x /999-chown-airflow-folder.sh && /999-chown-airflow-folder.sh
RUN rm /999-chown-airflow-folder.sh


# Create a volume for syncing files with the sidecar. The actual folder
# is created by the `001-create-mwaa-dir.sh` script.
VOLUME ["/usr/local/mwaa"]

# TODO We should only expose this port if the comand is 'webserver'.
EXPOSE 8080

ENV PATH=${PATH_AIRFLOW_USER}}
RUN unset PATH_DEFAULT
RUN unset PATH_AIRFLOW_USER

WORKDIR ${AIRFLOW_USER_HOME}

COPY entrypoint.py /entrypoint.py
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER airflow

ENTRYPOINT ["/entrypoint.sh"]

CMD /bin/bash