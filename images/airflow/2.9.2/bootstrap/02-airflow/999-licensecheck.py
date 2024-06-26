#!/usr/bin/env python3
"""
This module ensures that we are only using acceptable licenses in our repository.
"""

from collections import defaultdict
from typing import DefaultDict, List
import importlib.metadata as importlib_metadata

# Amazon Pre-Approved Licenses
ACCEPTABLE_LICENSES = [
    "AFL",
    "ASL",
    "Academic Free License",
    "Amazon",
    "Apache",
    "AppleJavaExtensions",
    "Artistic",
    "BSD",
    "Boost",
    "CC0",
    "Carnegie",
    "Creative Commons",
    "Curl",
    "Eiffel Forum",
    "Free Type",
    "HPND",
    "ISC",
    "ImageMagick",
    "Indiana University",
    "JPEG",
    "JSON",
    "JTidy",
    "JiBX",
    "Jython",
    "Libtiff",
    "MIT",
    "MSIS",
    "MX4J",
    "Microsoft Public",
    "NCSA",
    "OFL",
    "Open Font",
    "OpenLDAP",
    "OpenSSL",
    "PDDL",
    "PDWiki",
    "PHP",
    "PIL",
    "PSL",
    "PostgreSQL",
    "Public Domain",
    "Python Imaging Library",
    "Python Software Foundation",
    "Ruby",
    "SLF4J",
    "Scala",
    "University of Illinois",
    "Unlicense",
    "VIM",
    "W3C",
    "WTFPL",
    "WordNet",
    "ZPL",
    "Zlib",
    "Zope Public",
    "libpng",
]

# Amazon Policy doesn't allow the use of these licenses.
PROHIBITED_LICENSES = [
    "AGPLv3",
    "Affero General Public",
    "Apple Public",
    "BUSL",
    "Business Source License",
    "CC-NC",
    "CDLA",
    "CPAL",
    "CRAPL",
    "Comminuty Data License Agreement",
    "Common Public Attribution",
    "Commons Clause",
    "Community Research and Academic Programming",
    "Confluent",
    "EUPL",
    "Elastic",
    "Enna",
    "European Union Public",
    "GNU General Public License v3",
    "GNU Lesser General Public License v3",
    "GPLv3",
    "HFOIL",
    "HPL",
    "Host Public",
    "Hugging Face",
    "LGPLv3",
    "NASA Open Source",
    "NOSA",
    "ODbL",
    "Open Data Commons Open Database",
    "Parity",
    "Prosperity",
    "RealNetworks Public",
    "Redis",
    "Server Side Public",
    "UC Berkeley",
    "University of Wisconsin",
]

# These can be used as well, under certain conditions, so we use a different list to
# keep the distinction visibile.
EXEMPTED_LICENSES = [
    "Mozilla Public",
    "MPL",
]

# These packages, although having licenses that Amazon doesn't approve, are specifically
# exempted because the use case is approved.
EXEMPTED_PACKAGES = [
    "chardet",
    "paramiko",
    "pendulum",
    "psycopg2",
    "psycopg2-binary",
]


def extract_license_info(pkg_metadata: importlib_metadata.PackageMetadata) -> str:
    """
    Accepts an importlib MetaData object and extract license information
    from both License field and the Classifiers fields.
    """
    license: str = str(pkg_metadata.get("License", ""))  # type: ignore
    # Also check all Classifiers for License information
    for classifier in pkg_metadata.get_all("Classifier", []):  # type: ignore
        if "License" in classifier:
            license += f" - {classifier}"
    return license


def main():
    """
    Script entrypoint.
    """
    license_check_failed = False
    results: DefaultDict[str, List[str]] = defaultdict(list)

    for dist in importlib_metadata.distributions():
        pkg_name = dist.metadata["Name"]
        pkg_license = extract_license_info(dist.metadata)
        if pkg_name in EXEMPTED_PACKAGES:
            print(f"Package {pkg_name} has license {pkg_license} but is exempted.")
            continue
        if any(x in pkg_license for x in ACCEPTABLE_LICENSES):
            results[pkg_license].append(pkg_name)
            continue
        if any(x in pkg_license for x in EXEMPTED_LICENSES):
            results[pkg_license].append(pkg_name)
            continue
        if any(x in pkg_license for x in PROHIBITED_LICENSES):
            license_check_failed = True
            print(
                f"ERROR: The package {pkg_name} has prohibited license {pkg_license}."
            )
            continue
        license_check_failed = True
        print(
            f"ERROR: Package {pkg_name} has a non-pre-approved License: {pkg_license}. "
            "If you think the use case is justified here, please reach out to AppSec "
            "or Open Source teams to obtain an approval, and update one or more of the "
            "lists above accordingly."
        )

    for item in results:
        print(f"The following packages has approved {item} license:")
        print(results[item])

    if license_check_failed:
        exit(2)


if __name__ == "__main__":
    main()
else:
    print("This module cannot be imported.")
    exit(1)
