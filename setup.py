from setuptools import setup, find_packages
 
setup(
    name="adl-Customer-MDM-ETL",
    version='1.0.0',
    author="Raghavendran Ravichandran",
    author_email="raghavendran.ravichandran@alcon.com",
    description="Customer MDM Ingestion Framework",
    long_description_content_type="text/markdown",
    url="https://alconprod.atlassian.net/wiki/spaces/MASTER/pages/5863768311/Core+Architecture",
    packages=find_packages(),    
    classifiers=[
        "Programming Language :: Python :: 3"
    ],
    install_requires=[
        'boto3',
        'unicodecsv==0.14.1',
        's3fs',
        'pyspark'
        # 'fake-awsglue',
        # 'simple-salesforce==1.0.0',
        # 'salesforce-bulk==2.1.0',
    ],
    python_requires='>=3.6',
    zip_safe=False,
    test_suite='nose.collector',
    tests_require=['nose'],
)