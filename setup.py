from setuptools import setup, find_packages
setup(
    name = "CDR2DB",
    version = "0.1",
    packages = find_packages(),
    scripts = ['src/cdr2db.py'],

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    install_requires = ['sqlalchemy>=0.7',
                        'progress']
)
