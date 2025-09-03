from setuptools import setup, find_packages

setup(
    name="sbf_furnace",          # project/package name
    version="0.1.0",                 # arbitrary version
    packages=find_packages(),        # automatically include my_cool_project/
    install_requires=[
        "dagster",
        "dagster-webserver",
        "dagster-daemon",
        "pyModbusTCP",   # Modbus TCP client
        "retry",         # retry decorator
        "pyserial",      # serial comms
    ],
    python_requires=">=3.8",         # match your Python version
)
