"""
Generate a Dockerfile based on the Jinja2-templated Dockerfile.j2 file.

Dockerfile is very limited in nature, with just primitive commands. This
usually results in Dockerfiles becoming lengthy, repetitive, and error prone,
resulting in quality degradation. To work around this limitation, we use Jinja2
template engine which offers a lot of futures, e.g. if statements, for loops,
etc., and enable integration with Python (via data variables) resulting in a
way more powerful Dockerfile.

When executed, this script takes the Dockerfile.j2 and pass it to Jinja2 engine
to produce a Dockerfile. The reader is referred to the code below for a better
understanding of the working mechanism of this.
"""

import os
import sys
from datetime import datetime
from typing import Any, List
from pathlib import Path

try:
    from jinja2 import Environment, FileSystemLoader
except ImportError:
    print(
        """
jinja2 pip library is required. Please install it with:

pip3 install jinja2
""".strip()
    )
    sys.exit(1)


def raise_helper(msg: str) -> None:
    """
    Helper method to enable Jinja2 templates to raise an exception.
    """
    raise RuntimeError(msg)


def remove_repeated_empty_lines(text: str) -> str:
    """
    Removes repeated empty lines from a given text, leaving at most one empty
    line between non-empty lines.

    :param text: The input text from which repeated empty lines should be
    removed.

    :returns: The cleaned text with no more than one consecutive empty line.
    """
    lines = text.split(os.linesep)  # Split the text into lines
    previous_line_empty = False  # Track if the previous line was empty
    cleaned_lines: List[str] = []

    for line in lines:
        # Check if the current line is empty
        if not line.strip():
            if not previous_line_empty:
                # If the current line is empty but the previous one wasn't, add
                # the empty line
                cleaned_lines.append(line)
                previous_line_empty = True
        else:
            # If the current line is not empty, add it and reset the flag
            cleaned_lines.append(line)
            previous_line_empty = False

    # Join the cleaned lines back into a single string
    cleaned_text = os.linesep.join(cleaned_lines)
    return cleaned_text


def generate_dockerfile(
    image_root_dir: Path, template_filename: str, output_file: str, data: dict[str, Any]
) -> None:
    # Load Dockerfile Jinja template.
    file_loader = FileSystemLoader(image_root_dir)
    env = Environment(loader=file_loader, autoescape=True)
    env.globals["raise"] = raise_helper  # type: ignore
    template = env.get_template(template_filename)

    # Render the template and generate the Dockerfile
    output = template.render(data)
    with open(os.path.join(image_root_dir, "Dockerfiles", output_file), "w") as f:
        f.write(
            f"""
#
# WARNING: Don't change this file manually. This file is auto-generated from
# the Jinja2-templated Dockerfile.j2 file, so you need to change that file
# instead.
#
    """.strip()
        )
        f.write(os.linesep)
        f.write(os.linesep)
        f.write(remove_repeated_empty_lines(output))


def generate_base_dockerfile(image_root_dir: Path) -> None:
    """Generate the Dockerfile.base file based on the Dockerfile.base.j2
    template.

    We generate multiple Docker images for different purposes, as explained below under
    the documentation of `generate_derivative_dockerfiles`. However, these derivative
    images actually share most of the setup. So, to reduce build time and avoid
    duplication, we generate a "base" Docker image, and then derive the rest of the
    images from them.

    :param image_root_dir: The root directory of the Docker image, i.e. where the
      `Dockerfile` resides.
    """
    # Template data
    data = {
        "bootstrapping_scripts_root_firstpass": sorted(
            [
                os.path.join("bootstrap/01-root-firstpass", file.name)
                for file in (image_root_dir / "bootstrap/01-root-firstpass").iterdir()
                if file.is_file()
            ]
        ),
        "bootstrapping_scripts_airflow": sorted(
            [
                os.path.join("bootstrap/02-airflow", file.name)
                for file in (image_root_dir / "bootstrap/02-airflow").iterdir()
                if file.is_file()
            ]
        ),
        "bootstrapping_scripts_root_secondpass": sorted(
            [
                os.path.join("bootstrap/03-root-secondpass", file.name)
                for file in (image_root_dir / "bootstrap/03-root-secondpass").iterdir()
                if file.is_file()
            ]
        ),
    }

    template_name = "Dockerfile.base.j2"
    dockerfile_name = "Dockerfile.base"
    generate_dockerfile(image_root_dir, template_name, dockerfile_name, data)


def generate_derivative_dockerfiles(
    image_root_dir: Path, build_type: str = "standard", dev: bool = False
) -> None:
    """Generate a Dockerfile based on the given build arguments.

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

    template_name = "Dockerfile.derivatives.j2"
    dockerfile_name = "Dockerfile"
    if build_type != "standard":
        dockerfile_name = f"{dockerfile_name}-{build_type}"
    if dev:
        dockerfile_name = f"{dockerfile_name}-dev"
    data = {
        "bootstrapping_scripts_dev": (
            sorted(
                [
                    os.path.join("bootstrap-dev", file.name)
                    for file in (image_root_dir / "bootstrap-dev").iterdir()
                    if file.is_file()
                ]
            )
            if dev
            else []
        ),
        "build_type": build_type,
    }

    generate_dockerfile(image_root_dir, template_name, dockerfile_name, data)


def generate_airflow_dockerfiles(image_root_dir: Path):
    # Generate the base Dockerfile file (Dockerfile.base).
    generate_base_dockerfile(image_root_dir)

    # Generate the derivative Dockerfiles (multiple Dockerfiles based on
    # the build arguments.)
    for dev in [True, False]:
        for build_type in ["standard", "explorer", "explorer-privileged"]:
            generate_derivative_dockerfiles(
                image_root_dir, build_type=build_type, dev=dev
            )


def main():
    """Start execution of the script."""
    for x in Path(__file__).parent.iterdir():
        if not x.is_dir():
            continue
        generate_airflow_dockerfiles(x)


if __name__ == "__main__":
    main()
else:
    print("This module cannot be imported.")
    sys.exit(1)
