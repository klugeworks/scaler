#!/usr/bin/env python
from __future__ import division
import argparse
import logging
import os
import time
import statsd
import requests
import redis
from urlparse import urlparse
import json

logger = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser(description='Monitor redis queues, push metrics to statsd, scale marathon workers')
    parser._optionals.title = "Options"
    parser.add_argument('-n', '--namespace', help='Redis key namespace',
                        type=str, default='kluge')
    parser.add_argument('-l', '--lang', help='Redis queue languages & marathon job languages',
                        type=str, default='english')
    parser.add_argument('-r', '--redis-server', help='The Redis instance (hostname:port) to connect to for work.',
                        type=str, default='tcp://redis.marathon.mesos:6379')
    parser.add_argument('-m', '--marathon-server', help='The Marathon instance (hostname:port) for scaling.',
                        type=str, default='http://10.213.221.89:8080')
    parser.add_argument('-s', '--statsd-server', help='The statsd instance (hostname:port) for metrics.',
                        type=str, default='http://statsd.stats.marathon.mesos:31990')
    parser.add_argument('-p', '--poll', help='Poll interval',
                        type=int, default=10)
    parser.add_argument('-w', '--max_workers', help='Maximum number of workers',
                        type=int, default=20)
    return parser.parse_args()


def redis_connect(hostname, port):
    # Set up connection to Redis
    logger.info('Setting up connection to redis')
    redis_connection = redis.StrictRedis(host=hostname, port=port, db=0)
    logger.info('Connected to redis')
    return redis_connection


def get_in_length(redis_conn, namespace, lang):
    keyname = "q:in:%s:stt:%s" % (namespace, lang)
    queue_len = redis_conn.llen(keyname)
    return queue_len


def get_proc_length(redis_conn, namespace, lang):
    keyname = "q:proc:%s:stt:%s" % (namespace, lang)
    queue_len = redis_conn.llen(keyname)
    return queue_len


def get_done_length(redis_conn, namespace, lang):
    keyname = "q:done:%s:stt:%s" % (namespace, lang)
    queue_len = redis_conn.llen(keyname)
    return queue_len


def get_marathon_workers(marathon):
    r = requests.get(marathon + "/v2/apps/tokenizer/english")
    if r.status_code == 200:
        info = r.json()
        return int(info['app']['tasksRunning'])


def scale_tasks(marathon, num_tasks):
    config = {"instances": num_tasks}
    headers = {'Content-type': 'application/json'}
    r = requests.put(marathon + "/v2/apps/tokenizer/english?force=true", data=json.dumps(config),
                     headers=headers)
    if r.status_code == 200:
        info = r.json()
        return info


def get_scale_down_candidates(redis_conn, namespace, lang):
    keyname = "%s:stt:tok:%s:idle" % (namespace, lang)
    candidates = redis_conn.smembers(keyname)
    if not candidates:
        return []
    return list(candidates)


def clear_idle_workers(redis_conn, namespace, lang):
    keyname = "%s:stt:tok:%s:idle" % (namespace, lang)
    redis_conn.delete(keyname)


def get_active_deployments(marathon):
    cancel_r = requests.get("%s/v2/apps/tokenizer/english/" % marathon)
    if cancel_r.status_code == 200:
        deployments = cancel_r.json()['app']['deployments']
        return deployments


def remove_candidates(marathon, candidates, num_to_kill, redis_conn, namespace, lang):
    headers = {'Content-type': 'application/json'}
    deployments = get_active_deployments(marathon)
    if deployments:
        for deployment in deployments:
            requests.delete("%s/v2/deployments/%s?force=true" % (marathon, deployment['id']))
    infos = list()
    killed = 0
    for candidate in candidates:
        r = requests.delete("%s/v2/apps/tokenizer/english/tasks/%s?scale=true" % (marathon, candidate),
                            headers=headers)
        if r.status_code == 200:
            infos.append(r.json())
            killed += 1
        keyname = "%s:stt:tok:%s:idle" % (namespace, lang)
        redis_conn.srem(keyname, candidate)
        if num_to_kill == killed:
            break
    return infos


def main():
    args = parse_arguments()

    # Redis connection
    redis_server = urlparse(args.redis_server)
    redis_port = redis_server.port if redis_server.port is not None else 6379
    redis_conn = redis_connect(redis_server.hostname, redis_port)

    # statsd connection
    statsd_server = urlparse(args.statsd_server)
    statsd_port = statsd_server.port if statsd_server.port is not None else 31990
    statsd_conn = set_statsd(statsd_server.hostname, statsd_port)

    t_proc_len = get_proc_length(redis_conn, args.namespace, args.lang)
    t_num_workers = get_marathon_workers(args.marathon_server)

    err_high_water_mark = max(t_proc_len - t_num_workers, 0)
    iters_idle = 0
    while True:
        # Get queue lengths
        in_len = get_in_length(redis_conn, args.namespace, args.lang)
        proc_len = get_proc_length(redis_conn, args.namespace, args.lang)
        done_len = get_done_length(redis_conn, args.namespace, args.lang)

        num_workers = get_marathon_workers(args.marathon_server)

        print "IN: %s" % str(in_len)
        print "PROC: %s" % str(proc_len)
        print "DONE: %s" % str(done_len)
        print "ERR: %s" % str(err_high_water_mark)
        print "---------"
        print "MARATHON WORKERS: %s" % str(num_workers)

        # Scale up logic
        scale_up = False
        backup = in_len - num_workers
        if backup > 0:
            scale_up = True
            scale_up_to = min(args.max_workers, in_len)
            if num_workers < scale_up_to:
                if not get_active_deployments(args.marathon_server):
                    msg = scale_tasks(args.marathon_server, scale_up_to)
                    print "SCALING UP TO %s" % str(scale_up_to)
                    print msg
                else:
                    print "ALREADY IN AN ACTIVE DEPLOYMENT"

        if (proc_len - err_high_water_mark) > num_workers:
            err_high_water_mark += (proc_len - err_high_water_mark)

        # push metrics to statsd
        true_proc_len = proc_len - err_high_water_mark
        gauge('english.in', in_len)
        gauge('english.proc', true_proc_len)
        gauge('english.err', err_high_water_mark)
        gauge('english.done', done_len)
        gauge('english.workers', num_workers)

        # Scale down logic
        scale_down = False
        if (proc_len - err_high_water_mark) < num_workers:
            iters_idle += 1
            if iters_idle >= int(25 / args.poll):
                scale_down = True
        else:
            iters_idle = 0

        if scale_down and not scale_up:
            scale_down_to = proc_len - err_high_water_mark
            num_to_kill = num_workers - scale_down_to
            if scale_down_to < num_workers:
                print "REQUESTING SCALE DOWN TO %s" % scale_down_to
                candidates = get_scale_down_candidates(redis_conn, args.namespace, args.lang)
                msgs = remove_candidates(args.marathon_server, candidates, num_to_kill, redis_conn,
                                         args.namespace, args.lang)
                print "SCALED DOWN %s" % str(len(msgs))
                if msgs:
                    print msgs
        clear_idle_workers(redis_conn, args.namespace, args.lang)
        print
        time.sleep(args.poll)


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


if __name__ == '__main__':
    main()
