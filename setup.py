import setuptools
from tools import get_requirements, get_readme


def main():
    setuptools.setup(
        name="kara_storage",
        version="1.0.1",
        author="a710128",
        author_email="qbjooo@qq.com",
        description="Kara Storage SDK",
        long_description=get_readme(),
        long_description_content_type="text/markdown",
        url="https://git.thunlp.vip/kara/kara-row-storage",
        packages=setuptools.find_packages(exclude=("tools",)),
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
            "Operating System :: POSIX :: Linux",
            "Programming Language :: C++"
        ],
        python_requires=">=3.6",
        setup_requires=["wheel"],
        install_requires=get_requirements()
    )

if __name__ == "__main__":
    main()
