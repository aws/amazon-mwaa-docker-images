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


def main() -> None:
    """Entrypoint of the script."""
    # Load Dockerfile Jinja template.
    file_loader = FileSystemLoader('.')
    env = Environment(loader=file_loader)
    template = env.get_template('Dockerfile.j2')

    # Template data
    data = {
        'bootstrapping_scripts_root_firstpass': [
            os.path.join('/bootstrap/01-root-firstpass', name).strip()
            for name in sorted(os.listdir('./bootstrap/01-root-firstpass'))
        ],
        'bootstrapping_scripts_airflow': [
            os.path.join('/bootstrap/02-airflow', name).strip()
            for name in sorted(os.listdir('./bootstrap/02-airflow'))
        ],
        'bootstrapping_scripts_root_secondpass': [
            os.path.join('/bootstrap/03-root-secondpass', name).strip()
            for name in sorted(os.listdir('./bootstrap/03-root-secondpass'))
        ],
    }

    # Render the template and generate the Dockerfile
    output = template.render(data)
    with open('Dockerfile', 'w') as f:
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
    main()
else:
    print('This module cannot be imported.')
    sys.exit(1)
