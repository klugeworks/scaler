from setuptools import setup, find_packages

setup(
    name='kluge_scaler',
    version='1.0',
    packages=find_packages(exclude=['tests']),
    scripts=["scripts/monitor.py"],
    install_requires=['statsd', 'requests', 'redis']
)
