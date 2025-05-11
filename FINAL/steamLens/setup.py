from setuptools import setup, find_packages

setup(
    name="steamLens",
    version="1.0.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "streamlit",
        "pandas",
        "numpy",
        "dask[distributed]",
        "pyarrow",
        "sentence-transformers",
        "accelerate",
    ],
    python_requires=">=3.6",
    entry_points={
        'console_scripts': [
            'steamLens=steamLens.app:main',
        ],
    },
) 