from setuptools import setup, find_packages
import os
import re

def get_version():
    repo_root = os.path.abspath(os.path.dirname(__file__))
    init_path = os.path.join(repo_root, "FAIRLinked", "__init__.py")
    with open(init_path, "r") as f:
        init_content = f.read()
    match = re.search(r'__version__\s*=\s*[\'"]([^\'"]+)[\'"]', init_content)
    if match:
        return match.group(1)
    raise RuntimeError("Version not found in __init__.py")

setup(
    name='FAIRLinked',
    version=get_version(),
    description='Transform materials research data into FAIR-compliant RDF Data. Align your datasets with MDS-Onto and convert them into Linked Data, enhancing interoperability and reusability for seamless data integration. See the README or vignette for more information. This tool is used by the SDLE Research Center at Case Western Reserve University.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Van D. Tran, Brandon Lee, Henry Dirks, Ritika Lamba, Balashanmuga Priyan Rajamohan, Gabriel Ponon, Quynh D. Tran, Ozan Dernek, Yinghui Wu, Erika I. Barcelos, Roger H. French, Laura S. Bruckman',
    author_email='rxf131@case.edu',
    license='BSD-3-Clause',
    project_urls={
        'Documentation': 'https://fairlinked.readthedocs.io/en/latest/',
        'Source': 'https://github.com/cwru-sdle/FAIRLinked',
        'Tracker': 'https://github.com/cwru-sdle/FAIRLinked/issues',
        'Homepage': 'https://cwrusdle.bitbucket.io/'
    },
    packages=find_packages(),
    install_requires=[
        'rdflib>=7.0.0',
        'typing-extensions>=4.0.0',
        'pyarrow>=11.0.0',
        'openpyxl>=3.0.0',
        'pandas>=1.0.0',
        'cemento>=0.6.1',
        'fuzzysearch>=0.8.0',
        'tqdm>=4.0.0',
        'pyld>=2.0.3'
    ],
    extras_require={
        'dev': [
            'pytest',
            'pytest-cov'
        ]
    },
    python_requires='>=3.9.18',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Information Analysis'
    ],
    entry_points={
        'console_scripts': [
            'FAIRLinked=FAIRLinked.cli.__main__:main',
        ],
    }
)