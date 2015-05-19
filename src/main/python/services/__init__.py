import abc
import contextlib
import logging
import os
import os.path
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import uuid


logger = logging.getLogger(__name__)


def get_open_port():
    sock = socket.socket()
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def write_template(source_path, target_path, bindings):
    with open(source_path, 'r') as source:
        with open(target_path, 'w') as target:
            target.write(source.read().format(**bindings))


class ManagedProcess(threading.Thread):

    class ManagedProcessError(RuntimeError):

        def __init__(self, instance, message, *args, **kwargs):
            instance.stdout.delete = False
            instance.stderr.delete = False
            parts = [
                '%r failed with error: %s' % (instance, message),
                'Process logs were written to %s' % (instance.dir,)
            ]
            message = '\n'.join(parts)

            super(ManagedProcess.ManagedProcessError, self).__init__(message, *args, **kwargs)

    def __init__(self, name, args, env=None):
        threading.Thread.__init__(self)

        self.name = name
        self.args = args
        self.env = env

        self.logger = logging.LoggerAdapter(logger, {'service': self.name})
        self.started = threading.Event()
        self.stop_requested = False
        self.daemon = True

        self.dir = tempfile.mkdtemp(prefix=name + '_')
        self.stdout = tempfile.NamedTemporaryFile(prefix='stdout_', dir=self.dir, delete=False)
        self.stderr = tempfile.NamedTemporaryFile(prefix='stderr_', dir=self.dir, delete=False)

    def __repr__(self):
        return '%s(%r)' % (self.name, self.args)

    def run(self):
        self.logger.info('Starting')
        self.child = subprocess.Popen(
            self.args,
            env=self.env,
            bufsize=1,
            stdout=self.stdout,
            stderr=self.stderr
        )
        self.started.set()
        self.logger.info('Started with PID %s' % self.child.pid)
        self.child.wait()

        if not self.stop_requested:
            raise self.ManagedProcessError(
                self,
                'Crashed during startup with %s exit code' % self.child.returncode
            )

    def wait_for(self, pattern, timeout=30):
        give_up_at = time.time() + timeout

        self.started.wait(timeout)

        streams = [open(f.name, 'r') for f in (self.stdout, self.stderr)]

        self.logger.debug('Scanning process output for %s', pattern)
        while time.time() < give_up_at:
            if any(pattern in descriptor.readline() for descriptor in streams):
                self.logger.debug('Found %s output', pattern)
                return True
            elif self.child.poll() is not None:  # It crashed
                return

            time.sleep(0.1)

        raise self.ManagedProcessError(
            self,
            'Could not find %r in %d seconds, giving up.' % (pattern, timeout)
        )

    def stop(self, timeout=10):
        if not getattr(self, 'child', False):
            raise RuntimeError('Child process never started')

        self.logger.info('Stopping')
        self.stop_requested = True

        if self.child.poll() is None:
            self.logger.info('Stopping child process')
            self.child.terminate()
            self.logger.info('Sent SIGTERM to child process')

        self.logger.info('Waiting %s seconds for thread to end', timeout)
        self.join(timeout=timeout)

        if self.is_alive():
            self.logger.warning('Thread did not terminate, and is still alive')

        return self.child.returncode


class ServiceRunner(object):
    __metaclass__ = abc.ABCMeta


    class ServiceLoggingAdapter(logging.LoggerAdapter):

        @contextlib.contextmanager
        def executed(self, message, *args, **kwargs):
            self.info('%s...' % message, *args, **kwargs)
            yield
            self.info('Done!')

    def __init__(self, root, host, port):
        self.root_path = root
        self.host = host
        self.port = port
        self.logger = self.ServiceLoggingAdapter(
            logger, {
                'root': self.root_path,
                'host': self.host,
                'port': self.port,
            }
        )

    def subprocess(self, args, env, on_error):
        try:
            subprocess.check_call(
                args,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        except subprocess.CalledProcessError:
            self.logger.info(on_error)
            self.logger.info(proc.stdout.read())
            self.logger.info(proc.stderr.read())
            raise RuntimeError(on_error)

    @abc.abstractmethod
    def start(self):
        pass

    @abc.abstractmethod
    def stop(self):
        pass

    def setup(self):
        pass

    def teardown(self):
        pass

    def __enter__(self):
        self.setup()
        self.start()

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
        self.teardown()


class ZooKeeper(ServiceRunner):

    def __init__(self, *args, **kwargs):
        super(ZooKeeper, self).__init__(*args, **kwargs)
        self.zk_server_path = os.path.join(self.root_path, 'bin', 'zkServer.sh')

    def setup(self):
        self.tmp_dir = tempfile.mkdtemp()

        # Generate config
        self.template = os.path.join(self.root_path, 'conf', 'zookeeper.properties.template')
        self.properties_file = os.path.join(self.tmp_dir, 'zookeeper.properties')
        bindings = {'tmp_dir': self.tmp_dir, 'host': self.host, 'port': self.port}
        write_template(self.template, self.properties_file, bindings)

    def __repr__(self):
        parts = {
            'host': self.host,
            'port': self.port,
            'tmp_dir': self.tmp_dir,
        }
        return 'ZooKeeper(%s)' % ', '.join('%s=%s' for (k,v) in parts.items())

    def start(self):
        self.logger.info('Running local ZooKeeper instance... %r' % self)

        self.child = ManagedProcess(
            'ZooKeeper',
            args=[self.zk_server_path, 'start-foreground', self.properties_file]
        )

        with self.logger.executed('Starting'):
            self.child.start()
            self.child.wait_for('binding to port', timeout=5)

    def stop(self):
        with self.logger.executed('Stopping'):
            self.child.stop()
            self.child = None

    def teardown(self):
        shutil.rmtree(self.tmp_dir)


class Kafka(ServiceRunner):

    def __init__(self, root, host, port, broker_id, zk_host, zk_port, zk_chroot=None, replicas=1, partitions=2):
        super(Kafka, self).__init__(root, host, port)

        if zk_chroot is None:
            zk_chroot = 'kafka_' + str(uuid.uuid4())

        self.zk_host = zk_host
        self.zk_port = zk_port
        self.zk_chroot = zk_chroot

        self.broker_id = broker_id
        self.replicas = replicas
        self.partitions = partitions

        self.running = False

    def setup(self):
        self.tmp_dir = tempfile.mkdtemp()

        # Create directories
        os.mkdir(os.path.join(self.tmp_dir, 'logs'))
        os.mkdir(os.path.join(self.tmp_dir, 'data'))

        # Generate configs
        self.template = os.path.join(self.root_path, 'config', 'kafka.properties.template')
        self.properties_file = os.path.join(self.tmp_dir, 'kafka.properties')
        bindings = {
            'broker_id': self.broker_id,
            'host': self.host,
            'port': self.port,
            'tmp_dir': self.tmp_dir,
            'partitions': self.partitions,
            'replicas': self.replicas,
            'zk_host': self.zk_host,
            'zk_port': self.zk_port,
            'zk_chroot': self.zk_chroot
        }
        write_template(self.template, self.properties_file, bindings)

    def __run_class_args(self, *args):
        result = [os.path.join(self.root_path, 'bin', 'kafka-run-class.sh')]
        result.extend(args)
        return result

    def __run_class_env(self):
        env = os.environ.copy()
        log_config_path = os.path.join(self.root_path, 'config', 'log4j.properties')
        env['KAFKA_LOG4J_OPTS'] = '-Dlog4j.configuration=file:%s' % log_config_path
        return env

    def __repr__(self):
        parts = {
            'host': self.host,
            'port': self.port,
            'broker_id': self.broker_id,
            'zk_host': self.zk_host,
            'zk_port': self.zk_port,
            'zk_chroot': self.zk_chroot,
            'replicas': self.replicas,
            'partitions': self.partitions,
            'tmp_dir': self.tmp_dir,
        }
        return 'Kafka(%s)' % ', '.join('%s=%s' for (k,v) in parts.items())


    def start(self):
        args = self.__run_class_args(
            'org.apache.zookeeper.ZooKeeperMain',
            '-server',
            '%s:%d' % (self.zk_host, self.zk_port),
            'create',
            '/%s' % self.zk_chroot,
            'kafka-python'
        )
        env = self.__run_class_env()

        with self.logger.executed('Creating ZooKeeper chroot node'):
            self.subprocess(args, env, 'Failed to create ZooKeeper chroot node')

        self.child = ManagedProcess(
            'Kafka',
            args=self.__run_class_args('kafka.Kafka', self.properties_file),
            env=self.__run_class_env()
        )

        with self.logger.executed('Starting'):
            self.child.start()
            self.child.wait_for('[Kafka Server %d], started' % self.broker_id)
            self.running = True

    def stop(self):
        with self.logger.executed('Stopping'):
            self.child.stop()
            self.child = None
            self.running = False

    def teardown(self):
        shutil.rmtree(self.tmp_dir)


class Postgres(ServiceRunner):

    def __init__(self, root, host, port, **extra_config):
        super(Postgres, self).__init__(root, host, port)

        self.initdb_path = os.path.join(self.root_path, 'bin', 'initdb')
        self.server_path = os.path.join(self.root_path, 'bin', 'postgres')
        self.createdb_path = os.path.join(self.root_path, 'bin', 'createdb')

        self.extra_config = extra_config

    def setup(self):
        self.tmp_dir = tempfile.mkdtemp()

        with self.logger.executed('Initializing DB'):
            self.subprocess(
                (self.initdb_path, '-D', self.tmp_dir),
                os.environ.copy(),
                'Failed to init postgres database'
            )

    def __repr__(self):
        parts = {
            'host': self.host,
            'port': self.port,
            'tmp_dir': self.tmp_dir,
        }
        return 'Postgres(%s)' % ', '.join('%s=%s' for (k,v) in parts.items())

    @property
    def server_args(self):
        args = [self.server_path, '-D', self.tmp_dir, '-p', str(self.port), '-h', self.host]

        for pair in self.extra_config.items():
            args.append('-c')
            args.append('='.join(map(str, pair)))

        return args

    def start(self):
        with self.logger.executed('Starting'):
            self.child = ManagedProcess('Postgres', args=self.server_args)
            self.child.start()
            self.child.wait_for('database system is ready to accept connections')

    def createdb(self, name='test'):
        with self.logger.executed('Creating test DB'):
            self.subprocess(
                (self.createdb_path, '-p', str(self.port), '-h', self.host, name),
                os.environ.copy(),
                'Failed to create "%s" postgres database' % name
            )

    def stop(self):
        with self.logger.executed('Stopping'):
            self.child.stop()
            self.child = None

    def teardown(self):
        shutil.rmtree(self.tmp_dir)


def __on_localhost_with_any_port(cls):
    def _inner(*args, **kwargs):
        return cls(host='127.0.0.1', port=get_open_port(), *args, **kwargs)
    return _inner


LocalZooKeeper = __on_localhost_with_any_port(ZooKeeper)
LocalKafka = __on_localhost_with_any_port(Kafka)
LocalPostgres = __on_localhost_with_any_port(Postgres)
