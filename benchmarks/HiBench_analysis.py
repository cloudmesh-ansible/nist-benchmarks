

from cloudmesh_bench_api.bench import AbstractBenchmarkRunner
from cloudmesh_bench_api.bench import BenchmarkError
from cloudmesh_bench_api import providers

from pxul.os import in_dir
from pxul.os import env as use_env
from pxul.subprocess import run

import sys
import os
import re
import time

import logging
logger = logging.getLogger(__name__)



class BenchmarkRunner(AbstractBenchmarkRunner):

    repo = 'git@github.com:Prash74/example-project-HiBench_new'
    name_prefix = '{}-{}-'.format(os.getlogin(), 'hibench-analysis')


    def _fetch(self, prefix):

        with in_dir(prefix):

            reponame = os.path.basename(self.repo)

            if not os.path.exists(reponame):
                run(['git', 'clone', '--recursive', self.repo])

            return os.path.join(os.getcwd(), reponame)


    def _generate_data(self, params):
        """Configure the data generation method

        Valid parameters:

        - method (:class:`str`): one of {lognormal|rmat}
        - nodes: (:class:`int`): number of nodes in the graph
        - edges: (:class:`int`): number of edges in the graph
        - mu: (:class:`float`): mu parameter for lognormal graph
        - sigma: (:class:`float`): sigma parameter for lognormal graph
        - nodesfile: (:class:`str`): path to save the nodes on HDFS
        - edgesfile: (:class:`str`): path to save the edges on HDFS
        - metric (:class:`str`): metric to calculate during analysis

        If no parameters are specified the default values will be used.

        :type params: :class:`dict` of :class:`str` to values
        """

        def replace_param(name, value, string):
            logger.debug('Replacing %s with %s', name, value)

            # this mirrors the vars defined in spark-submit.sh
            names = dict(
                method = 'NA_METHOD',
                method_args = 'NA_METHOD_ARGS',
                nodesfile = 'NA_NODESFILE',
                edgesfile = 'NA_EDGESFILE',
                metric = 'NA_METRIC',
            )


            if name in names.keys():
                varname = names[name]
            elif name in ['nodes', 'edges']:
                varname = names['method_args']
            else:
                msg = "I don't know how to set parameter {}".format(name)
                logger.error(msg)
                raise ValueError(msg)

            pattern = '({}=")(.*)(" *### REGEXP REPLACE)'.format(varname)

            match = re.search(pattern, string)
            if not match:
                msg = 'Unable to match pattern {} on string \n{}'.format(pattern, string)
                raise ValueError(msg)

            current_value = match.groups(2)
            logger.debug('Current value: %s', current_value)

            if name in ['nodes', 'edges']:

                if name == 'nodes':
                    flag = '-n'
                elif name == 'edges':
                    flag = '-e'

                if flag + ' ' not in current_value:
                    new_value = '{} {}'.format(flag, value)
                else:
                    value_pattern = r'({} \d+)'.format(flag)
                    assert re.match(value_pattern, current_value)
                    new_value = re.sub(value_pattern,
                                       r'{} {}'.format(flag, value),
                                       current_value)

            else:
                assert name in names.keys()
                new_value = "{}".format(value)

            logger.info('New value: %s', new_value)
            new_string = re.sub(pattern, r'\1{}\3'.format(new_value), string)
            return new_string

    def _prepare(self):

        with in_dir(self.path):
            run(['virtualenv', 'venv'])
            new_env = self.eval_bash(['deactivate',
                                      'source venv/bin/activate'])

            with use_env(**new_env):
                run(['pip', 'install', '-r', 'requirements.txt', '-U'])

        return new_env


    def _configure(self, node_count=3,remote_user='cc',os='Ubuntu-14.04-64',size='small'):
        if node_count < 3:
            msg = 'Invalid node count {} is less than {}'\
                  .format(node_count, 3)
            raise BenchmarkError(msg)

        ########################################### node count

        new_count = 'N_NODES = {}'.format(node_count)
        old_count = re.compile(r'N_NODES = \d+')
        with in_dir(self.path):
            with open('.cluster.py') as fd:
                cluster = fd.read()
            new_cluster = old_count.sub(new_count, cluster)
            with open('.cluster.py', 'w') as fd:
                fd.write(new_cluster)

            new_user = 'remote_user={}'.format(remote_user)
            old_user = re.compile(r'remote_user=\S+')
            with open('ansible.cfg') as fd:
                user = fd.read()
            new_cfg = old_user.sub(new_user, user)
            with open('ansible.cfg', 'w') as fd:
                fd.write(new_cfg)

            new_user = 'remote_user: {}'.format(remote_user)
            old_user = re.compile(r'remote_user: \S+')
            with open('plays/vars.yml') as fd:
                user = fd.read()
            new_cfg = old_user.sub(new_user, user)
            with open('plays/vars.yml', 'w') as fd:
                fd.write(new_cfg)

            #Changing Data Size
            new_size = 'scale: {}'.format(size)
            old_size = re.compile(r'scale: \S+')
            with open('plays/vars.yml') as fd:
                user = fd.read()
            new_cfg = old_size.sub(new_size, user)
            print new_cfg
            with open('plays/vars.yml', 'w') as fd:
                fd.write(new_cfg)


    def _launch(self):

        with in_dir(self.path):
            run(['vcl', 'boot', '-p', 'openstack', '-P',
                 self.name_prefix])

            for i in xrange(12):
                result = run(['ansible', 'all', '-m', 'ping', '-o',
                              '-f', str(self.node_count)],
                             raises=False)
                if result.ret == 0:
                    return
                else:
                    time.sleep(5)

            msg = 'Timed out when waiting for nodes to come online'
            raise BenchmarkError(msg)

    def _deploy(self):

        with in_dir(self.path):
            run(['ansible-playbook',
                 'play-hadoop.yml',
                 'addons/spark.yml'
            ])


    def _run(self):
        with in_dir(self.path):
            run(['ansible-playbook',
                 'hibench.yml'])


    def _verify(self):
        return True


    def _clean_openstack(self):

        with in_dir(self.path):
            result = run(['vcl', 'list'], capture='stdout',
                         raises=False)
            node_names = map(str.strip, result.out.split())
            node_names = ['%s%s' % (self.name_prefix, n) for n in
                          node_names]
            cmd = ['nova', 'delete'] + node_names
            run(cmd, raises=False)

            while True:
                nova_list = run(['nova', 'list', '--fields', 'name'],
                                capture='stdout')
                present = any([n in nova_list.out for n in node_names])
                if present:
                    time.sleep(5)
                else:
                    break


    def _clean(self):

        if self.provider_name == providers.openstack:
            self._clean_openstack()

print "Project Name : HiBench Analysis using Naive Bayes and K-Means"
print "Cloud Platform :",sys.argv[1]
print "Base Operating System :",sys.argv[2]

import logging
logging.basicConfig(level=logging.DEBUG)

CHAMELEON_OPENRC_FILES = [
    '~/.cloudmesh/clouds/india/kilo/chameleon.sh'
]

b = BenchmarkRunner(prefix='projects', node_count=3,
                    files_to_source=CHAMELEON_OPENRC_FILES,
                    provider_name='openstack',
                    data_params=dict(nodes=10),
)
b.bench(times=2)
print b.report.pretty()
