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
import stats

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
    r = requests.get(marathon + "/v2/apps/tokenizer/english/tasks")
    if r.status_code == 200:
        info = r.json()
        return len(info['tasks'])


def scale_tasks(marathon, num_tasks):
    config = {"instances": num_tasks}
    headers = {'Content-type': 'application/json'}
    r = requests.put(marathon + "/v2/apps/tokenizer/english?force=true", data=json.dumps(config),
                     headers=headers)
    if r.status_code == 200:
        info = r.json()
        return info


def get_scale_down_candidates(redis_conn, namespace, lang):
    keyname = "%s:stt:%s:idle" % (namespace, lang)
    candidates = redis_conn.smembers(keyname)
    return candidates


def remove_candidates(marathon, candidates, redis_conn, namespace, lang):
    infos = list()
    for candidate in candidates:
        headers = {'Content-type': 'application/json'}
        r = requests.delete("%s/v2/apps/tokenizer/english/tasks/%s?scale=true" % (marathon, candidate),
                            headers=headers)
        if r.status_code == 200:
            keyname = "%s:stt:%s:idle" % (namespace, lang)
            redis_conn.srem(keyname, candidate)
            infos.append(r.json())
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
    statsd_conn = stats.set_statsd(statsd_server.hostname, statsd_port)

    t_proc_len = get_proc_length(redis_conn, args.namespace, args.lang)
    t_num_workers = get_marathon_workers(args.marathon_server)

    err_high_water_mark = t_proc_len - t_num_workers
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
                msg = scale_tasks(args.marathon_server, scale_up_to)
                print "SCALING UP TO %s" % str(scale_up_to)
                print msg

        if (proc_len - err_high_water_mark) > num_workers:
            err_high_water_mark += (proc_len - err_high_water_mark)

        # push metrics to statsd
        true_proc_len = proc_len - err_high_water_mark
        stats.gauge('english.in', in_len)
        stats.gauge('english.proc', true_proc_len)
        stats.gauge('english.err', err_high_water_mark)
        stats.gauge('english.done', done_len)
        stats.gauge('english.workers', num_workers)
        # Scale down logic
        scale_down = False
        if (proc_len - err_high_water_mark) < num_workers:
            iters_idle += 1
            if iters_idle >= int(60 / args.poll):
                scale_down = True
        else:
            iters_idle = 0

        if scale_down and not scale_up:
            scale_down_to = proc_len - err_high_water_mark
            if scale_down_to < num_workers:
                print "REQUESTING SCALE DOWN TO %s" % scale_down_to
                candidates = get_scale_down_candidates(redis_conn, args.namespace, args.lang)
                msgs = remove_candidates(args.marathon, candidates[:scale_down_to])
                print "SCALING DOWN TO %s" % str(len(msgs))
                print msgs

        print
        time.sleep(args.poll)

if __name__ == '__main__':
    main()
