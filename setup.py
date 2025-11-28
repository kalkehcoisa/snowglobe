"""
Snowglobe - Local Snowflake Emulator for Python Developers
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="snowglobe",
    version="0.1.0",
    author="Snowglobe Contributors",
    author_email="jaymetosineto@gmail.com",
    description="Local Snowflake Emulator for Python Developers",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kalkehcoisa/snowglobe",
    packages=find_packages(include=["snowglobe_server", "snowglobe_server.*"]),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Topic :: Software Development :: Testing",
    ],
    python_requires=">=3.8",
    install_requires=[
        "fastapi>=0.104.0",
        "uvicorn[standard]>=0.24.0",
        "pydantic>=2.0.0",
        "duckdb>=0.9.0",
    ],
    extras_require={
        "pandas": ["pandas>=1.5.0"],
        "client": [
            "snowflake-connector-python>=3.0.0",
        ],
        "test": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "pytest-asyncio>=0.21.0",
            "httpx>=0.24.0",
            "tox>=4.0.0",
        ],
        "dev": [
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
            "isort>=5.12.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "snowglobe-server=snowglobe_server.server:main",
        ],
    },
    project_urls={
        "Bug Reports": "https://github.com/kalkehcoisa/snowglobe/issues",
        "Source": "https://github.com/kalkehcoisa/snowglobe",
        "Documentation": "https://github.com/kalkehcoisa/snowglobe/wiki",
    },
)
