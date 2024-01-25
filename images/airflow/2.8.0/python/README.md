This folder gets copied over to the Docker image under the `/python` path.
Additionally, the path `/path` is added to the `PYTHONENVIRONMENT` environment
variable. As such, all the Python files under this folder are importable from
any python code.
