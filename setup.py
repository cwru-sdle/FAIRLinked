from setuptools import setup, find_packages

setup(
    name='FAIRLinked',
    version='0.3.0.9',
    description='Transform research data into FAIR-compliant RDF using the RDF Data Cube Vocabulary. Align your datasets with MDS-Onto and convert them into Linked Data, enhancing interoperability and reusability for seamless data integration. See the README or vignette for more information. This tool is used by the SDLE Research Center at Case Western Reserve University.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Ritika Lamba, Van Tran, Balashanmuga Priyan Rajamohan, Kai Zheng, Benjamin Pierce, Erika I. Barcelos, Roger H. French',
    author_email='rxf131@case.edu',
    license='BSD-3-Clause',
    packages=find_packages(),
    install_requires=[
        'rdflib>=7.0.0',
        'typing-extensions>=4.0.0',
        'pyarrow>=11.0.0',
        'openpyxl>=3.0.0',
        'pandas>=1.0.0',
        'cemento>=0.6.1',
        'fuzzysearch>=0.8.0',
        'tqdm>=4.0.0'
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
)