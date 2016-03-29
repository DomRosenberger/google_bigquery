import os
import sys

from setuptools import setup, find_packages


def readme():
    with open('README.rst') as f:
        return f.read()


# Publish Helper.
if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

setup(name='google_bigquery',
      version='0.13',
      description='Wrapper for Google\'s Bigquery Python API.',
      keywords='bigquery google',
      url='http://github.com/Nobody109/google_bigquery',
      author='Dominik Rosenberger',
      author_email='dominik.rosenberger@gmail.com',
      license='MIT',
      packages=find_packages(exclude=['tests', 'test*']),
      install_requires=[
          'google-api-python-client',
      ],
      test_suite='nose.collector',
      tests_require=['nose', 'mox'],
      include_package_data=True,
      zip_safe=False)
