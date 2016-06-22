

from cloudmesh_bench_api.bench import AbstractBenchmarkRunner
from cloudmesh_bench_api.bench import BenchmarkError
from cloudmesh_bench_api import providers

from pxul.os import in_dir
from pxul.os import env as use_env
from pxul.subprocess import run

import os
import re
import time



class BenchmarkRunner(AbstractBenchmarkRunner):

    repo = 'git@github.com:cloudmesh/example-project-network-analysis'
    name_prefix = '{}-{}-'.format(os.getlogin(), 'network-analysis')


    def _fetch(self, prefix):

        with in_dir(prefix):

            reponame = os.path.basename(self.repo)

            if not os.path.exists(reponame):
                run(['git', 'clone', '--recursive', self.repo])

            return os.path.join(os.getcwd(), reponame)


    def _prepare(self):

        with in_dir(self.path):
            run(['virtualenv', 'venv'])
            new_env = self.eval_bash(['deactivate',
                                      'source venv/bin/activate'])

            with use_env(**new_env):
                run(['pip', 'install', '-r', 'requirements.txt', '-U'])

        return new_env


    def _configure(self, node_count=3):
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
                 '-f', str(self.node_count),
                 'play-hadoop.yml',
                 'addons/pig.yml', 'addons/spark.yml',
                 'deploy.yml'
            ])


    def _run(self):
        with in_dir(self.path):
            run(['ansible-playbook',
                 '-f', str(self.node_count),
                 'run.yml'])


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


import logging
logging.basicConfig(level=logging.DEBUG)

CHAMELEON_OPENRC_FILES = [
    '~/.cloudmesh/clouds/chameleon/CH-817724-openrc.sh'
]

b = BenchmarkRunner(prefix='projects', node_count=3,
                    files_to_source=CHAMELEON_OPENRC_FILES,
                    provider_name='openstack'
)
b.bench(times=5)
print b.report.pretty()
