from setuptools import setup, find_packages

setup(
    name="crashboard-sdk",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "fastapi",
        "prometheus-fastapi-instrumentator",
        "confluent-kafka",
        "redis",
        "docker",
        "pyyaml",
        "jinja2",
        "click",
        "requests"
    ],
    entry_points={
        "console_scripts": [
            "crashboard=crashboard.cli:main",
        ],
    },
    author="CrashBoard Team",
    description="Chaos-as-a-Service SDK for microservices observability and fault injection",
)
