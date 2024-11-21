from setuptools import find_packages, setup

setup(
    name="threads9703e8c4640bd",
    version="0.0.6",
    packages=find_packages(),
    install_requires=[
        "exorde_data",
        "selenium==4.2.0",
        "beautifulsoup4==4.12.3",
        "requests"
    ],
    extras_require={"dev": ["pytest", "pytest-cov", "pytest-asyncio"]},
)
