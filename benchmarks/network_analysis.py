

from cloudmesh_bench_api.bench import AbstractBenchmarkRunner

from pxul.os import in_dir
from pxul.os import env as use_env
from pxul.subprocess import run

import os
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
    openrc = '~/.cloudmesh/clouds/chameleon/CH-817419-openrc.sh'


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


    def _launch(self):

        with in_dir(self.path):
            run(['vcl', 'boot', '-p', 'openstack', '-P', 'badi-'])

            for i in xrange(12):
                result = run(['ansible', 'all', '-m', 'ping', '-o'],
                             raises=False)
                if result.ret == 0:
                    break
                else:
                    time.sleep(5)


    def _deploy(self):

        with in_dir(self.path):
            run(['ansible-playbook',
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
