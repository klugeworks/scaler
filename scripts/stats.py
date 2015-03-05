import os
import logging

_statsd = None
logger = logging.getLogger(__name__)


def set_statsd(statsd_server, statsd_port=8125):
    try:
        import statsd
    except ImportError:
        logger.warn('statsd not installed. Cannot aggregate statistics')
        return

    global _statsd
    port = statsd_port
    server = statsd_server.split(':', 1)
    hostname = server[0]
    if len(server) == 2:
        port = int(server[1])
    try:
        _statsd = statsd.StatsClient(hostname, port, prefix='redis-q')
    except Exception, e:
        logger.error(str(e))
        return
    logger.debug("Aggregating statistics to {0}:{1}".format(hostname, port))


statsd_server_env = 'KLUGE_STATSD_SERVER'
if statsd_server_env in os.environ:
    set_statsd(os.environ[statsd_server_env])


def incr(stat, count=1, rate=1):
    if _statsd:
        _statsd.incr(stat, count=count, rate=rate)


def decr(stat, count=1, rate=1):
    if _statsd:
        _statsd.decr(stat, count=count, rate=rate)


def timing(stat, delta, rate=1):
    if _statsd:
        _statsd.timing(stat, delta, rate=rate)


def gauge(stat, value, rate=1, delta=False):
    if _statsd:
        _statsd.gauge(stat, value, rate=rate, delta=delta)