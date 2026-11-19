## aws-mwaa-docker-images

## Overview

This repository contains the Docker Images that [Amazon
MWAA](https://aws.amazon.com/managed-workflows-for-apache-airflow/) uses to run Airflow.

You can also use it locally if you want to run a MWAA-like environment for testing, experimentation,
and development purposes.

Currently, Airflow v2.9.2 and above are supported. Future versions in parity with Amazon MWAA will be added as
well. _Notice, however, that we do not plan to support previous Airflow versions supported by MWAA._

## Using the Airflow Image

### Linux / macOS

To experiment with the image using a vanilla Docker setup, follow these steps:

0. _(Prerequisites)_ Ensure you have:
   - Python 3.11 or later.
   - [Docker](https://docs.docker.com/desktop/) and [Docker Compose](https://docs.docker.com/compose/install/)
1. Clone this repository.
2. This repository makes use of Python virtual environments. To create them, from the root of the
   package, execute the following command:

```
# Create venvs for all Airflow versions
python3 create_venvs.py --target <development | production>

# Or create venv for a specific version only
python3 create_venvs.py --target <development | production> --version 3.0.6
```

3. Build and run a supported Airflow version Docker image:
   - `cd <amazon-mwaa-docker-images path>/images/airflow/2.9.2`
   - `./run.sh`

   By default this runs fully locally (no AWS account needed): it builds the images, starts the containers, and writes logs inside the container at `/usr/local/airflow/logs/`. The web server is at `http://localhost:8080`.

   To build an image for a different architecture than your host's, set `PLATFORM` to `linux/amd64` or `linux/arm64`, for example `PLATFORM=linux/amd64 ./build.sh docker` on an Apple Silicon host. `run.sh` passes `PLATFORM` to the build and to `test-dags`, `test-requirements`, `test-startup-script` and `package-requirements`, for example `PLATFORM=linux/amd64 ./run.sh test-requirements`. The full local stack always runs on the host's architecture, so `run.sh` rejects `PLATFORM` for other commands. Without `PLATFORM`, images are built and run for the host's architecture. A cross-architecture build needs emulation (Docker Desktop includes it) and is several times slower.

   `run.sh` also writes the values its compose files read, including the AWS credentials and the Fernet key, to a `.env` file in the version directory, because `finch compose` runs inside the Finch virtual machine and doesn't see your shell's variables. The file is ignored by Git, readable only by you, and rewritten on every run, replacing any `.env` you keep there. Any `compose` command you run in that directory also uses it. With Finch on Apple Silicon, an amd64 build needs Rosetta: set `rosetta: true` in `~/.finch/finch.yaml` and recreate the virtual machine with `finch vm stop && finch vm remove && finch vm init`. Under Finch's default QEMU emulation, the Airflow 3.3.1 build fails in `002-install-duckdb-extensions.sh` with exit code 139. With Finch, `test-dags`, `test-requirements` and `test-startup-script` need Finch 1.19.0 or later. Earlier versions can bundle a `finch compose` without the `--abort-on-container-exit` and `--force-recreate` options these commands use; check your version with `finch version`.

   To publish logs to CloudWatch instead, set `ACCOUNT_ID`, `ENV_NAME`, and real AWS credentials in `run.sh`. `./run.sh` then creates and writes to these log groups (your credentials need permission to do so):
     - `{ENV_NAME}-DAGProcessing`
     - `{ENV_NAME}-Scheduler`
     - `{ENV_NAME}-Worker`
     - `{ENV_NAME}-Task`
     - `{ENV_NAME}-WebServer`

   The credentials you provide are also used by the Airflow components at runtime, so if you hit permission errors, grant the needed permissions to that identity.

---

### Windows (PowerShell 5.1) [Not supported by AWS MWAA Service Team]

#### Prerequisites

- Windows 10/11 with PowerShell 5.1 (built-in — no installation needed)
- Python 3.11 or later — install from [python.org](https://www.python.org/downloads/) (check "Add Python to PATH" during install)
- [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/) set to **Linux containers** mode
  - Right-click the Docker Desktop tray icon → "Switch to Linux containers" if needed
- AWS CLI (optional — only required if you want CloudWatch log group creation)

#### One-time setup

1. Clone this repository.

2. Allow PowerShell to run local scripts (run once as your user):
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

3. Create the Python virtual environments from the repo root:
```powershell
# Create venvs for all Airflow versions
python create_venvs.py --target development

# Or for a specific version only
python create_venvs.py --target development --version <version>
```

#### Running

4. Navigate to an Airflow version directory and run:
```powershell
cd images\airflow\<version>
.\run.ps1
```

This will build the Docker images and start the full Airflow stack. On first run, the image build can take 10–20 minutes.

- To test a `requirements.txt` without running Airflow:
```powershell
.\run.ps1 -Command test-requirements
```
- To test a `startup.sh` without running Airflow:
```powershell
.\run.ps1 -Command test-startup-script
```

#### AWS Credentials

For local development without a real AWS account, `run.ps1` defaults to dummy values — ElasticMQ (the local SQS mock) does not validate credentials. To use real AWS services (e.g. CloudWatch logging), update the `$AccountId`, `$EnvName`, and `$env:AWS_*` values at the top of `run.ps1`.

#### Logging in

Once the stack is up, open `http://localhost:8080`. The default credentials are printed in the webserver container logs on startup.

#### Adding DAGs

Drop DAG files into `images\airflow\<version>\dags\`. They are live-mounted into the container — no restart needed. The scheduler picks them up within a minute or two.

#### Stopping

```powershell
docker compose down
```

#### Troubleshooting

| Problem | Fix |
|---|---|
| `cannot be loaded because running scripts is disabled` | Run `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser` |
| `Docker is not in Linux containers mode` | Right-click Docker Desktop tray icon → Switch to Linux containers |
| `python` not found | Install Python 3.11+ from python.org with "Add to PATH" checked |
| `Unable to locate credentials` | Ensure `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are non-empty in `run.ps1` |
| Login fails at `http://localhost:8080` | Check the webserver container logs for the credentials printed on startup |
| DAG not appearing | Check the scheduler container logs or verify the file exists in the `dags\` folder |

---

### Authentication from version 3.0.1 onward
For environments created using this repository starting with version 3.0.1, we default to using `SimpleAuthManager`, 
which is also the default auth manager in Airflow 3.0.0+. By default, `SIMPLE_AUTH_MANAGER_ALL_ADMINS` is set to true, 
which means no username/password is required, and all users will have admin access. You can specify users and roles 
using the SIMPLE_AUTH_MANAGER_USERS environment variable in the format:
```
username:role[,username2:role2,...]
```
To enforce authentication with explicit user passwords and roles, set:

```
SIMPLE_AUTH_MANAGER_ALL_ADMINS=false
```
In this mode, a password will be automatically generated for each user and printed in the webserver logs as soon as 
webserver starts.

---

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

---

## Extra commands

To check DAGs, plugins and requirements against a target Airflow version before an upgrade, see [Validate DAGs before an Amazon MWAA upgrade](#validate-dags-before-an-amazon-mwaa-upgrade).

#### Requirements

For details on installing Python depedencies, and optionally bundling wheel files, see the [Managing Python dependencies in requirements.txt](https://docs.aws.amazon.com/mwaa/latest/userguide/best-practices-dependencies.html#best-practices-dependencies-different-ways) in the Amazon MWAA user guide.  

- Add Python dependencies to `requirements/requirements.txt`
- To test a `requirements.txt` without running Apache Airflow, run:
```bash
./run.sh test-requirements
```
- The command exits 0 when the requirements install (or there are none) and 2 when they fail to install.

#### Package requirements

- To download all packages from your `requirements.txt` as wheel (`.whl`) and source distribution (`.tar.gz`) files and bundle them into a `plugins.zip` for offline installation on MWAA, run:
```bash
./run.sh package-requirements
```
- This produces two artifacts in the `requirements/` directory:
  - `plugins.zip` — all downloaded packages bundled into a single ZIP.
  - `packaged_requirements.txt` — your original requirements prefixed with `--no-index` and `--find-links` flags for offline installation.
- Upload `plugins.zip` to your MWAA S3 bucket and use `packaged_requirements.txt` as your requirements file. For more details, see [Installing Python dependencies using wheel files](https://docs.aws.amazon.com/mwaa/latest/userguide/best-practices-dependencies.html#best-practices-dependencies-different-ways) in the Amazon MWAA user guide.

#### Startup script

- There is a folder in each airflow version called `startup_script`. Add your script there as `startup.sh`
- If there is a need to run additional setup (e.g. install system libraries, setting up environment variables), please modify the `startup.sh` script.
- To test a `startup.sh` without running Apache Airflow, run:
```bash
./run.sh  test-startup-script
```
- The command exits 0 after the script runs. It exits 1 only if the script times out or its environment variables can't be read. MWAA doesn't treat errors from commands inside the script as fatal, so check the output for them.

#### Reset database

- If you encountered [the following error](https://issues.apache.org/jira/browse/AIRFLOW-3678): `process fails with "dag_stats_table already exists"`, you'll need to reset your database. You just need to restart your container by exiting and rerunning the `run.sh` script

## Validate DAGs before an Amazon MWAA upgrade

Use the image for the Airflow version you plan to upgrade to, and check your DAGs, plugins, requirements and startup script against it. The checks run locally. They don't need an AWS account or a metadata database.

### Build the image once

```bash
cd images/airflow/<version>
./build.sh docker   # or podman, or finch
```

The first build of one version took about 5 minutes on a 16-core x86_64 Linux host with a fast network. Expect longer on a laptop. Later builds reuse cached layers. `./run.sh` also runs `build.sh` before each command.

To build for a different architecture than your host's, set `PLATFORM` to `linux/amd64` or `linux/arm64`, and use the same value for `run.sh`:

```bash
PLATFORM=linux/amd64 ./build.sh docker
PLATFORM=linux/amd64 ./run.sh test-dags --dags ~/my-environment/dags
```

A cross-architecture build needs emulation (Docker Desktop includes it) and is several times slower. Use the architecture your environment uses when you validate requirements, because some packages ship different wheels per architecture.

### Commands

```bash
./run.sh test-requirements [--requirements <file>]
./run.sh test-startup-script [--startup-script <file>]
./run.sh test-dags [--dags <dir>] [--plugins <dir or plugins.zip>] [--requirements <file>] [--startup-script <file>] [--report <file>]
```

Without flags, each command uses this version folder's `dags/`, `plugins/`, `requirements/requirements.txt` and `startup/startup.sh`. Paths may be relative to the current folder. Inputs are mounted read-only.

- `--requirements` mounts the file's folder, so `-r` and `-c` lines that name files next to it keep working.
- `--plugins` takes a folder or a `plugins.zip`. A zip is unzipped into the plugins folder, as MWAA does.
- `--report` sets where the JSON report is written. It must be a new file: if anything already exists at that path, `run.sh` exits 64, so it never overwrites a file. The default is a new `./validation-report-YYYYMMDD-HHMMSS.json` in the current folder. The report is readable only by you, and if the check stops before writing it, the empty file is removed.
- Each run starts a fresh container, so packages and plugins from an earlier run don't carry over.

`test-dags` does the following without starting Airflow or a database:

1. Runs the startup script, if there is one.
2. Copies or unzips the plugins.
3. Installs the requirements, the same way `test-requirements` does.
4. Loads the plugins, then parses each DAG file in its own process with Python's default warning filters, so deprecation warnings are shown.
5. Writes the report and prints a summary.

Two time limits keep a run from hanging. Starting Airflow and loading the plugins must finish within 5 minutes. Each DAG file must finish parsing within `dagbag_import_timeout` (30 seconds in these images) plus 10 seconds, or it's reported with an import error. If the 5-minute limit runs out, the run exits 1 with `error` set in the report.

To see a report with problems in it, run the sample files in this repository from a version folder: `./run.sh test-dags --dags ../../../tests/fixtures/dag_validation/dags --plugins ../../../tests/fixtures/dag_validation/plugins`. It exits 1 and reports each problem.

### Exit codes

| Code | Meaning |
|---|---|
| 0 | Requirements installed (or there are none), every plugin loaded and every DAG file parsed. Warnings don't change the code. |
| 1 | At least one DAG import error or plugin import error, or the check couldn't finish (see `error` in the report). |
| 2 | The requirements failed to install. DAGs aren't parsed. |
| 64 | `run.sh` got an unknown flag, a path that doesn't exist, or a `--report` path that already exists. Nothing ran. |

`test-requirements` returns 0 or 2. `test-startup-script` returns 0 unless the script times out or its environment variables can't be read.

### Report

```json
{
  "schema_version": 1,
  "airflow_version": "3.3.1",
  "python_version": "3.12.16",
  "error": null,
  "parse_mode": "fork",
  "requirements": {"status": "ok", "error": null},
  "plugins": {"import_errors": [{"file": "bad_plugin.py", "error": "No module named 'missing_plugin_dependency'"}]},
  "dags": [
    {
      "file": "old_import_dag.py",
      "dag_ids": ["old_import_dag"],
      "import_error": null,
      "import_error_traceback": null,
      "warnings": [
        {
          "category": "DeprecatedImportWarning",
          "message": "The `airflow.operators.bash.BashOperator` attribute is deprecated. Please use `'airflow.providers.standard.operators.bash.BashOperator'`.",
          "lineno": 5,
          "location": "old_import_dag.py:5"
        }
      ]
    }
  ],
  "summary": {"files": 1, "dags": 1, "import_errors": 0, "plugin_import_errors": 1, "warnings": 1}
}
```

- `requirements.status` is `ok`, `failed`, or `skipped` (no requirements file, or the check stopped before installing).
- `dags[].file` and `plugins.import_errors[].file` are relative to the DAGs and plugins folders.
- `dags[].import_error` is the exception line. `import_error_traceback` has the full traceback.
- `warnings[].lineno` is the line in the DAG file when the warning points at it, and `null` otherwise. `location` always shows where Python pointed, as `path:line`.
- `parse_mode` is `fork`, or `subprocess` if more than one thread was running (for example because a plugin started one), which makes forking unsafe. Parsing is then slower but gives the same results.
- Fields in `schema_version` 1 keep their names and meaning. New fields may be added.

### What the check can't catch

- Failures that only happen when a task runs, such as bad operator arguments, missing connections or variables, or provider behavior at run time.
- Code that reaches the metadata database other than at parse time. A DAG file that queries the database while it's parsed fails here with an import error, because no database is running.
- Files that DAG discovery skips. In safe mode (the default), Airflow only parses files that contain both `airflow` and `dag`, and only those files appear in the report.
- Import-time warnings from modules that were imported before parsing started, for example helpers that a plugin imports.
- Errors from commands in a startup script. MWAA doesn't treat them as fatal, so `test-startup-script` still returns 0. Read its output.
- On Airflow 3.2 and later, warnings raised in a DAG file before it fails to import aren't reported, because Airflow's importer drops them.
- Airflow configuration overrides from your environment aren't applied. The check uses this image's defaults, for example for `dag_discovery_safe_mode` and `dagbag_import_timeout`.
- Code that calls AWS while a DAG file or plugin is imported. DAG and plugin code gets no AWS credentials during the check, so the call fails and the file gets an import error, for example `NoCredentialsError`. On MWAA the same call would use the execution role. boto3 and the AWS CLI also skip the EC2 instance metadata service, but code that calls that service directly isn't blocked.
- Files the container can't read. It runs as uid 1000, so DAG and plugin files must be readable by that user.

### Troubleshooting

- **A Docker Desktop pull hangs in a non-interactive shell.** A `credsStore` or `credHelpers` entry, or a hook, in `~/.docker/config.json` can wait for a prompt that never shows. Use a temporary config without them:
  ```bash
  export DOCKER_CONFIG="$(mktemp -d)"
  echo '{}' > "$DOCKER_CONFIG/config.json"
  # If `docker compose` is no longer found, link your CLI plugins:
  ln -s ~/.docker/cli-plugins "$DOCKER_CONFIG/cli-plugins"
  ```
- **A build fails on `FROM localhost/...` or can't pull a `-base` image.** The derivative Dockerfiles start from the base image that `build.sh` builds and tags locally. Always build with `build.sh` (or `run.sh`), not with `docker build` on a single Dockerfile.
- **A folder named `validation-report.json` appears.** Run the test commands through `run.sh`, not `docker compose` directly. `run.sh` creates the report file before the container starts; otherwise Docker creates a folder at that path.
- **Apple Silicon.** A native build gives an arm64 image. To test against x86_64 packages, build and run with `PLATFORM=linux/amd64`, which uses emulation and is slower.
- **Podman and Finch.** With Finch, `run.sh` reads the exit code from the stopped container, because `finch compose` has no `--exit-code-from`, and passes the flags' paths to `finch compose` through `.env`. Exit codes were verified with Docker Compose v2 and podman-compose. Finch was checked only with a stand-in for `finch compose`.
- **Windows.** `run.ps1` and `build.ps1` don't have the new flags, `PLATFORM` or `test-dags` yet.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
