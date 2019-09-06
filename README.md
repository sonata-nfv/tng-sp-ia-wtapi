[![Build Status](http://jenkins.sonata-nfv.eu/buildStatus/icon?job=tng-vnv-curator/master)](https://jenkins.sonata-nfv.eu/job/tng-vnv-curator) [![Gitter](https://badges.gitter.im/sonata-nfv/Lobby.svg)](https://gitter.im/sonata-nfv/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

<p align="center"><img src="https://github.com/sonata-nfv/tng-api-gtw/wiki/images/sonata-5gtango-logo-500px.png" /></p>

# 5GTANGO Transport API (T-API) WIM wrapper

This is a component in SONATA [5GTANGO](http://www.5gtango.eu) service platform responsible to manage the interface between MANO and WAN managers implementing TAPI.

T-API plugin is integrated in the Infrastructure Abstraction component and manages WIMs using T-API protocol. It is using an extended version of T-API, that allows the use of flow matching parameters (e.g., match per link-layer address or per IP address). Also, MPLS tags are used for each virtual link.

## Installing / Getting started

To install

```shell
python -m pip install --upgrade pip setuptools wheel
python setup.py install
```

To execute it

```shell
tng-sp-ia-wtapi
```


To run it integrated with the rest of SONATA as a docker container:

```shell
docker pull registry.sonata-nfv.eu:5000/tng-sp-ia-wtapi
docker run -d --name tng-sp-ia-wtapi registry.sonata-nfv.eu:5000/tng-sp-ia-wtapi
```
It is recommended to use the [quick guide](https://sonata-nfv.github.io/sp-installation) to install the whole SONATA Service Platform.

## Developing

### Built with

The application is developed in python 3.6 and  has subscribing threads assigned to IA-message queue channels, and when a message arrives the attached logic is executed.

The whole list of python libraries uses is included in setup.py:

 * amqpstorm
 * pytest
 * PyYAML
 * requests
 * pycodestyle
 * coloredlogs
 * psycopg2-binary
 
### Prerequisites

It is needed python3.6 It is recommended to have **virtualenv** installed and setup a workspace separated from the whole system.

### Setting up Dev

Setting up a development environment is easy

```shell
git clone https://github.com/sonata-nfv/tng-sp-ia-wtapi.git
cd tng-sp-ia-wtapi/

# Optionally setup a virtual environment
virtualenv -p python3 venv
source venv/bin/activate

python -m pip install --upgrade pip setuptools wheel
```

### Building

To install the module (after seting up dev)

```shell
python setup.py develop
```

### Deploying / Publishing

```shell
tng-sp-ia-wtapi
```

## Versioning

For the versions available, see the [link to tags on this repository](https://github.com/sonata-nfv/tng-sp-ia-wtapi/releases).

## Api Reference

There is no REST API but there are three channels used by plugin to communicate with IA core module:

 * infrastructure.tapi.management.wan.list
 * infrastructure.tapi.service.wan.configure
 * infrastructure.tapi.service.wan.deconfigure

## Dependencies

`docker (18.x)`

## Contributing
For contributing to the T-API plugin you must:

1. Clone [this repository](http://github.com/sonata-nfv/tng-sp-ia-wtapi);
1. Work on your proposed changes, preferably through submiting [issues](https://github.com/sonata-nfv/tng-sp-ia-wtapi/issues);
1. Submit a Pull Request;
1. Follow/answer related [issues](https://github.com/sonata-nfv/tng-sp-ia-wtapi/issues) (see Feedback, below).

## Licensing

This 5GTANGO component is published under Apache 2.0 license. Please see the [LICENSE](LICENSE) file for more details.

## Lead Developers

The following lead developers are responsible for this repository and have admin rights. They can, for example, merge pull requests.

* Juan Luis de la Cruz ([juanlucruz](https://github.com/juanlucruz))

Reviewers:

* Felipe Vicens ([felipevicens](https://github.com/felipevicens))
* Carlos Parada ([carlos-f-parada](https://github.com/carlos-f-parada))

## Feedback-Channel

- You may use the mailing list [sonata-dev-list](mailto:sonata-dev@lists.atosresearch.eu)
- Gitter room [![Gitter](https://badges.gitter.im/sonata-nfv/Lobby.svg)](https://gitter.im/sonata-nfv/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)


