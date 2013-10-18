from setuptools import setup

setup(
    name="Elliptics GatlingGun",
    version="0.1",
    url="https://github.com/nobodyisme/gatlinggun",
    author="Andrey Vasilenkov",
    author_email="indigo@yandex-team.ru",
    packages=['gatlinggun',
              'gatlinggun.inventory'],
    package_dir={'gatlinggun': 'src/gatlinggun'},
    license="LGPLv3+",
    scripts=[]
)
