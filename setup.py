import os
import setuptools
from tools import get_requirements, get_readme, get_version

path = os.path.dirname(os.path.abspath(__file__))

version = get_version()

open( os.path.join(path, "kara_storage", "version.py"), "w" ).write('version = "%s"' % version)



def main():
    setuptools.setup(
        name="kara_storage",
        version=version,
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
            "Programming Language :: C++"
        ],
        python_requires=">=3.6",
        setup_requires=["wheel"],
        scripts=["scripts/kara_storage"],
        install_requires=get_requirements()
    )

if __name__ == "__main__":
    main()
