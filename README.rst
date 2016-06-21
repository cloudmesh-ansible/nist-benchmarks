=============
 Description
=============


This repository houses the code for running benchmarks for several NIST-related projects.
The projects involve launching a virtual cluster on some provider (eg OpenStack, Comet, EC2), deploying necessary software and datasets, running some analysis code on the dataset, and cleaning up.


========
 Status
========

This is a Work-In-Progress and considered unstable.
The instructions below reflect this state and will be updated as the repository matures.


===============
 Prerequisites
===============

#. An account on github.com
#. An ssh key uploaded to github.com
#. A Python development environment (pip)

   .. note::

      This has only been tested with Python 2.7

#. Credentials to cloud providers stored in a bash-sourceable file


.. note::

   We recommend using a virtual environment. Please install the
   appropriate ``virtualenv`` package for this.


=======
 Usage
=======


#. Download this repository::

     git clone git@github.com:cloudmesh/nist-benchmarks.git
     cd nist-benchmarks

#. Setup the virtual environment::

     virtualenv venv
     source venv/bin/activate

#. install dependencies::

     pip install -r https://raw.githubusercontent.com/cloudmesh/bench-api-utils/master/requirements.txt
     pip install -e git+https://github.com/cloudmesh/bench-api-utils#egg=cloudmesh_bench_api



You can run the network-analysis benchmark like so::

  python benchmarks/network_analysis.py


