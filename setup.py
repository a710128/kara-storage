import os
import setuptools
from pathlib import Path

def get_requirements():
    ret = []
    with open(Path(__file__).parent.parent.joinpath("requirements.txt")) as freq:
        for line in freq.readlines():
            ret.append( line.strip() )
    return ret
    
def get_readme():
    ret = ""
    with open(Path(__file__).parent.parent.joinpath("README.md")) as frd:
        ret = frd.read()
    return ret

def get_version():
    if "CI_COMMIT_TAG" in os.environ:
        return os.environ["CI_COMMIT_TAG"]
    if "CI_COMMIT_SHA" in os.environ:
        return os.environ["CI_COMMIT_SHA"]
    return "test"

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
