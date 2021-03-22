import setuptools
from tools import get_requirements, get_readme


def main():
    setuptools.setup(
        name="kara_storage",
        version="0.0.1",
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
        install_requires=get_requirements(),
        include_package_data=True,
        ext_modules=[
            setuptools.Extension("kara_storage._C.local_dataset",
                extra_compile_args=["-g","-Wall","-std=c++11"],
                sources = [
                    "csrc/python_local_dataset.cpp", 
                    "csrc/local_trunk_controller.cpp", 
                    "csrc/dataset.cpp"
                ],
                include_dirs = [
                    "csrc/includes"
                ],
            )
        ]
    )

if __name__ == "__main__":
    main()