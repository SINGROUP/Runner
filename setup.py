from setuptools import setup, find_packages
import re


with open('runner/__init__.py') as fd:
    version = re.search("__version__ = '(.*)'", fd.read()).group(1)


if __name__ == '__main__': 
    setup(
        name="runner",
        version=version,
        packages=find_packages(),
        # scripts=["say_hello.py"],
        install_requires=["docutils>=0.3",
                          "ase",
                          "numpy",
                          "psutil"],
        entry_points={'console_scripts': ['runner=runner.cli:main']},
        package_data={
            # If any package contains *.txt or *.rst files, include them:
            "": ["*.txt", "*.rst"],
        },
    )
