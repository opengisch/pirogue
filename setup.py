from setuptools import setup
import sys
if sys.version_info < (3,3):
    sys.exit('Pirogie requires at least Python version 3.3.\nYou are currently running this installation with\n\n{}'.format(sys.version))

setup(
    name = 'pum',
    packages = [
        'pirogue'
    ],
    scripts = [
        'scripts/pirogue'
    ],
    version = '[VERSION]',
    description = 'PostgreSQL view generator',
    author = 'Denis Rouzaud',
    author_email = 'denis@opengis.ch',
    url = 'https://github.com/opengisch/pirogue',
    download_url = 'https://github.com/opengisch/pirogue/archive/[VERSION].tar.gz', # I'll explain this in a second
    keywords = [
        'postgres'
    ],
    classifiers = [
        'Topic :: Database',
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Information Technology',
        'Development Status :: 3 - Alpha'
    ],
    install_requires = [
        'psycopg2-binary>=2.7.3'
    ],
    python_requires=">=3.3",
)
