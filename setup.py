from setuptools import setup, find_packages

if __name__ == '__main__': 
    setup(
        name="runner",
        version="0.1",
        packages=find_packages(),
        # scripts=["say_hello.py"],
        install_requires=["docutils>=0.3",
                          "ase",
                          "numpy"],
        package_data={
            # If any package contains *.txt or *.rst files, include them:
            "": ["*.txt", "*.rst"],
        },
    )
