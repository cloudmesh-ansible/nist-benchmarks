

from cloudmesh_bench_api.bench import AbstractBenchmarkRunner
from cloudmesh_bench_api.bench import BenchmarkError

from pxul.os import in_dir
from pxul.os import env as use_env
from pxul.subprocess import run

import os
import re
import time


def bash(commands, filename='__bash.sh'):
    script = '\n'.join(commands)

    with open(filename, 'w') as fd:
        fd.write(script)

    return filename


def get_env(commands):
    commands = [ '%s >/dev/null 2>&1' % c for c in commands ]
    script = bash(commands + ['env'])

    new_env = dict()

    output = run(['bash', script], capture='stdout').out.split('\n')
    for l in output:
        line = l.strip()
        if not line: continue
        if '=' not in line: continue
        k, v = line.split('=', 1)
        new_env[k] = v
    os.unlink(script)

    return new_env


class BenchmarkRunner(AbstractBenchmarkRunner):

    repo = 'git@github.com:cloudmesh/example-project-network-analysis'
    openrc = '~/.cloudmesh/clouds/chameleon/CH-817724-openrc.sh'


    def _fetch(self, prefix):

        with in_dir(prefix):

            reponame = os.path.basename(self.repo)

            if not os.path.exists(reponame):
                run(['git', 'clone', '--recursive', self.repo])

            return os.path.join(os.getcwd(), reponame)


    def _prepare(self):

        with in_dir(self.path):
            run(['virtualenv', 'venv'])
            new_env = get_env(['deactivate',
                               'source venv/bin/activate',
                               'source %s' % self.openrc])

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
            run(['vcl', 'boot', '-p', 'openstack', '-P', 'benchmark-'])

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
            run(['ansible-playbook', 'run.yml'])


    def _verify(self):
        return True


    def _clean(self):
        with in_dir(self.path):
            result = run(['vcl', 'list'], capture='stdout',
                         raises=False)
            node_names = map(str.strip, result.out.split())
            for n in node_names:
                run(['nova', 'delete', 'badi-%s' % n], raises=False)



import logging
logging.basicConfig(level=logging.DEBUG)

b = BenchmarkRunner()
b.bench(times=3)
print b.report.pretty()
