# noisepy-seis-io
[![PyPI](https://img.shields.io/pypi/v/noisepy-seis-io?color=blue&logo=pypi&logoColor=white)](https://pypi.org/project/noisepy-seis-io/)
[![codecov](https://codecov.io/gh/noisepy/noisepy-io/graph/badge.svg?token=3YIRLLXVmE)](https://codecov.io/gh/noisepy/noisepy-io)

This project was automatically generated using the LINCC-Frameworks [python-project-template](https://github.com/lincc-frameworks/python-project-template). For more information about the project template see the
[documentation](https://lincc-ppt.readthedocs.io/en/latest/).

## Development Guide

Before installing any dependencies or writing code, it's a great idea to create a virtual environment. LINCC-Frameworks engineers primarily use `conda` to manage virtual environments. If you have conda installed locally, you can run the following to create and activate a new environment.

```
conda create env -n noisepy python=3.10
conda activate noisepy
```

Once you have created a new environment, you can install this project for local development and below are the recommended steps for setting up your environment based on different installation scenarios:

1. Installing from PyPI:

    ```
    pip install noisepy-seis
    ```

If you're using pip install noisepy-seis to install the package directly from PyPI, all sources and dependencies will be placed in the appropriate site-packages directory. This setup is suitable for production environments and does not require additional setup for development.

2. Installing in Editable mode:

    ```
    pip install -e .[dev]
    ```

If you're cloning the noisepy-seis repository and installing the development version in editable mode (`-e` flag), you can follow these steps:

- Clone the noisepy-seis repository from GitHub.
- Install the package in editable mode by running `pip install -e .[dev]`.
- Any additional steps or workarounds specific to this scenario can be added here.

3. Installing without Editable Mode:

    ```
    pip install .[dev]
    ```

4. Install pre-commit hook:

    ```
    pre-commit install
    ```

Note that `pre-commit install` will initialize pre-commit for this local repository, so that a set of tests will be run prior to completing a local commit. For more information, see the Python Project Template documentation on [pre-commit](https://lincc-ppt.readthedocs.io/en/latest/practices/precommit.html).
