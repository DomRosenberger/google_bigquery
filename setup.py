from setuptools import setup, find_packages


def readme():
    with open('README.rst') as f:
        return f.read()

setup(name='google_bigquery',
      version='0.11',
      description='Wrapper for Google\'s Bigquery Python API.',
      keywords='bigquery google',
      url='http://github.com/Nobody109/google_bigquery',
      author='Dominik Rosenberger',
      author_email='dominik.rosenberger@gmail.com',
      license='MIT',
      packages=find_packages(exclude=['tests']),
      install_requires=[
          'google-api-python-client',
      ],
      test_suite='nose.collector',
      tests_require=['nose', 'mox'],
      include_package_data=True,
      zip_safe=False)
