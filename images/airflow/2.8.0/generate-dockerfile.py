"""
Generate a Dockerfile based on the Jinja2-templated Dockerfile.j2 file.

Dockerfile is very limited in nature, with just primitive commands. This
usually results in Dockerfiles becoming lengthy, repetetive, and error prone,
resulting in quality degradation. To work around this limitation, we use Jinja2
templating engine which offers a lot of futures, e.g. if statements, for loops,
etc., and enable integration with Python (via data variables) resulting in a
way more powerful Dockerfile.

When exectued, this script takes the Dockerfile.j2 and pass it to Jinja2 engine
to produce a Dockerfile. The reader is referred to the code below for a better
understanding of the working mechanism of this.
"""

import os
import sys
from datetime import datetime

try:
    from jinja2 import Environment, FileSystemLoader
except ImportError:
    print('''
jinja2 pip library is required. Please install it with:

pip3 install jinja2
'''.strip())
    sys.exit(1)


def raise_helper(msg) -> None:
    """
    Helper method to enable Jinja2 templates to raise an exception.
    """
    raise RuntimeError(msg)


def is_dev_bootstrapping_step(bootstrap_filename: str) -> bool:
    """
    Determines whether the given bootstrap filename is supposed to run only in
    development images. This is decided based on the prefix "devonly-" in the
    filename directly following the index prefix (the numbers at the
    beginning.) For example, the file `200-devonly-install-dev-tools.sh`
    matches this criteria and will be only executed for building development
    images.

    :param bootstrap_filename: The name of the bootstrapping file.

    :return True or False.
    """
    comps = bootstrap_filename.split('-')
    return len(comps) > 1 and comps[1] == 'devonly'


def generate_dockerfile(output_file, build_type='standard', dev=False) -> None:
    """
    Generate a Dockerfile based on the given build arguments.

    :param build_type: Specifies the build type. This can have the following
      values:
      - standard: This is the standard build type. it is what customer uses.
      - explorer: The 'explorer' build type is almost identical to the
      'standard' build type but it doesn't include the entrypoint. This is
      useful for debugging purposes to run the image and look around its
      content without starting airflow, which might require further setup.
      - explorer-root: This is similar to the 'explorer' build type, but
      additionally uses the root user, giving the user of this Docker image
      elevated permissions.  The user can, thus, install packages, remove
      packages, or anything else.

    :param dev: Whether to produce a development image or a production one.
      Development images have extra stuff that are useful during development,
      e.g. editors, sudo, etc.
    """
    # Load Dockerfile Jinja template.
    file_loader = FileSystemLoader('.')
    env = Environment(loader=file_loader)
    env.globals['raise'] = raise_helper
    template = env.get_template('Dockerfile.j2')

    # Template data
    data = {
        'bootstrapping_scripts_root_firstpass': [
            os.path.join('/bootstrap/01-root-firstpass', name).strip()
            for name in sorted(os.listdir('./bootstrap/01-root-firstpass'))
            if not is_dev_bootstrapping_step(name) or dev is True
        ],
        'bootstrapping_scripts_airflow': [
            os.path.join('/bootstrap/02-airflow', name).strip()
            for name in sorted(os.listdir('./bootstrap/02-airflow'))
            if not is_dev_bootstrapping_step(name) or dev is True
        ],
        'bootstrapping_scripts_root_secondpass': [
            os.path.join('/bootstrap/03-root-secondpass', name).strip()
            for name in sorted(os.listdir('./bootstrap/03-root-secondpass'))
            if not is_dev_bootstrapping_step(name) or dev is True
        ],
        'build_type': build_type
    }

    # Render the template and generate the Dockerfile
    output = template.render(data)
    with open(os.path.join('./Dockerfiles', output_file), 'w') as f:
        f.write(f'''
#
# WARNING: Don't change this file manually. This file is auto-generated from
# the Jinja2-templated Dockerfile.j2 file, so you need to change that file
# instead.
#
# This file was generated on {datetime.now()}
#
    '''.strip())
        f.write(os.linesep)
        f.write(os.linesep)
        f.write(output)


if __name__ == '__main__':
    for dev in [True, False]:
        for build_type in ['standard', 'explorer', 'explorer-privileged']:
            dockerfile_name = 'Dockerfile'
            if build_type != 'standard':
                dockerfile_name = f'{dockerfile_name}-{build_type}'
            if dev:
                dockerfile_name = f'{dockerfile_name}-dev'
            generate_dockerfile(dockerfile_name,
                                build_type=build_type, dev=dev)
else:
    print('This module cannot be imported.')
    sys.exit(1)
