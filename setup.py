from setuptools import setup
import sys

python_min_version = (3, 6)

if sys.version_info < python_min_version:
    sys.exit('Pirogue requires at least Python version {vmaj}.{vmin}.\n'
             'You are currently running this installation with\n\n{curver}'.format(
        vmaj=python_min_version[0],
        vmin=python_min_version[1],
        curver=sys.version))

setup(
    name = 'pirogue',
    packages = [
        'pirogue',
        'scripts'
    ],
    entry_points={
        'console_scripts': [
            'pirogue = scripts.pirogue:main'
        ]
    },    version = '[VERSION]',
    description = 'PostgreSQL view generator',
    author = 'Denis Rouzaud',
    author_email = 'denis.rouzaud@gmail.com',
    url = 'https://github.com/opengisch/pirogue',
    download_url = 'https://github.com/opengisch/pirogue/archive/[VERSION].tar.gz',
    keywords = ['postgres'],
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
    python_requires=">={vmaj}.{vmin}".format(vmaj=python_min_version[0], vmin=python_min_version[1]),
)
