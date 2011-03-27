#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import re
import sys
import json
import pycurl
import urllib
from dateutil.parser import parse
from datetime import datetime
from threading import Timer
from optparse import OptionParser
from StringIO import StringIO
from urlparse import urlunsplit
from urllib import urlencode

RAILS_REQUEST_LOG_FORMAT = r'''^\s*Started\ (?P<method>(GET|POST))\ "(?P<uri>.*)"\ for\ (?P<ip>\d+\.\d+\.\d+\.\d+)\ at\ (?P<time>.*)$
^\s*Processing\ by\ .*\ as\ (?P<format>(JSON|HTML|MANIFEST))$
(^\s*Parameters:\ (?P<parameters>{.*})$)?'''

ANDROID_USER_AGENT = 'Mozilla/5.0 (Linux; U; Android 2.1; en-us; Nexus One Build/ERD62) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Mobile Safari/530.17'

IPHONE_USER_AGENT = 'Mozilla/5.0 (iPhone; U; CPU like Mac OS X; en) AppleWebKit/420+ (KHTML, like Gecko) Version/3.0 Mobile/1A543a Safari/419.3'

class RequestLogReplayer(object):
    """
    Simulate HTTP requests according to Rails request log

    """
    def __init__(self, log_format=RAILS_REQUEST_LOG_FORMAT, verbose=False):
        self.log_format = re.compile(log_format, re.M)
        self.verbose = verbose

    def parse_log(self, logs, host=None):
        """
        Parse requests from log file
        
        Rails log format:
        
        Started GET "/sitemaps/xs/1.xml" for 127.0.0.1 at Thu Mar 24 14:04:35 +0800 2011
          Processing by SitemapsController#books as XML
          Parameters: {"page"=>"1"}
    
        Result format:
    
        [{
            "host": "www.zuikong.net",
            "uri": "/sitemaps/xs/1.xml",
            "format": "XML",
            "method": "GET",
            "parameters": {"page": 1},
            "time": datetime(2011, 3, 24, 14, 4, 35),
            "interval": 0,
        }, { ... }, ...]
    
        >>> log = '''Started POST "/replay/http/requests" for 127.0.0.1 at 2011-03-17 11:53:55 +0800
        ...   Processing by Replay::Http#requests as JSON
        ...   Parameters: {"now"=>"1300333994418", "v"=>"2", "entries"=>{"0"=>{"id"=>"4d5e2cea28de605001006945", "version"=>"0", "complete"=>"true"}}}
        ... Completed   in 3ms
        ... '''
        >>> replayer = RequestLogReplayer()
        >>> request = replayer.parse_log([log], host='www.zuikong.net')[0]
        >>> request['host']
        'www.zuikong.net'
        >>> request['uri']
        '/replay/http/requests'
        >>> request['format']
        'JSON'
        >>> request['method']
        'POST'
        >>> request['time']
        datetime.datetime(2011, 3, 17, 11, 53, 55, tzinfo=tzoffset(None, 28800))
        >>> request['interval']
        0
        >>> request['parameters']['now']
        u'1300333994418'

        """
        requests = []
        start_time = None
        for log in logs:
            match = self.log_format.search(log)
            if match:
                request = match.groupdict()
                request['host'] = host

                # convert time string into datetime object
                request['time'] = parse(request['time'])
                if start_time is None or request['time'] < start_time:
                    start_time = request['time']

                if request.get('parameters'):
                    # convert ruby-style hash string into dict
                    parameters = request['parameters'].replace('=>', ':')
                    try:
                        request['parameters'] = json.loads(parameters)
                    except ValueError, e:
                        # Omit the parameters if failed to parse some rails unicode
                        request['parameters'] = {}
                else:
                    request['parameters'] = {}
                requests.append(request)

        # Generate interval
        for request in requests:
            request['interval'] = (request['time'] - start_time).seconds
        return requests
    
    def replay_requests(self, requests, concurrency=1, speed=1.0):
        """
        Build request thread according to log
    
        >>> replayer = RequestLogReplayer()
        >>> requests = [
        ...     {"host": "www.google.com", "uri": "/1", "method": "GET", "format": "JSON", "interval": 0, "time": datetime(2011, 3, 25, 14, 32, 0), "parameters": {}},
        ...     {"host": "www.baidu.com", "uri": "/2", "method": "GET", "format": "HTML", "interval": 5, "time": datetime(2011, 3, 25, 14, 32, 5), "parameters": {"v": 1}},
        ... ]
        >>> replayer.replay_requests(requests, speed=0.5, concurrency=3)
        Started replays.
        <BLANKLINE>
        Replayed GET "/1" on "www.google.com" at 2011-03-25 14:32:00
        Processed as JSON
        Completed at 2011-03-24 14:43:00
        <BLANKLINE>
        Replayed GET "/2" on "www.baidu.com" at 2011-03-25 14:32:05
        Processed as HTML
        Completed at 2011-03-24 14:43:05
        <BLANKLINE>

        """
        # Group requests by intervals
        groups = {}
        for request in requests:
            if request['interval'] in groups:
                groups[request['interval']].append(request)
            else:
                groups[request['interval']] = [request]

        timers = [self._create_timer(interval, group, concurrency, speed) \
                for interval, group in groups.items()]
        print("Started %s replays.\n" % (len(requests) * concurrency,))
        for timer in timers:
            timer.start()

    def send_requests(self, requests):
        for request in requests:
            self.send_request(request)

    def send_request(self, request):
        """
        Send a HTTP request to server
    
        >>> replayer = RequestLogReplayer(verbose=False)
        >>> request = {'host': 'www.zuikong.com', 'uri': '/xs/46/ping.json', 'method': 'POST', 'format': 'JSON', 'time':  datetime(2011, 3, 25, 14, 32, 5), 'parameters': {'v': 2}}
        >>> replayer.send_request(request)
        >>> request = {'host': 'www.zuikong.com', 'uri': '/xs/46/ping.json', 'method': 'GET', 'format': 'JSON', 'time':  datetime(2011, 3, 25, 14, 32, 5), 'parameters': {'v': 2}}
        >>> replayer.send_request(request)
        {
          "title": "\u51e1\u4eba\u4fee\u4ed9\u4f20",
          "version": 1712,
          "article_title": "\u7b2c\u4e00\u5343\u516d\u767e\u516b\u5341\u516b\u7ae0",
          "article_url": "http://www.zuikong.com/xs/46/zj/1071172"
        }

        """
        start_time = datetime.now()
        curl = pycurl.Curl()
        data = urlencode(request['parameters'])

        # Setup url and parameters
        if request['method'] == 'GET':
            curl.setopt(pycurl.URL, urlunsplit(('http', request['host'], request['uri'], data, '')))
        elif request['method'] == 'POST':
            curl.setopt(pycurl.URL, urlunsplit(('http', request['host'], request['uri'], '', '')))
            curl.setopt(pycurl.POST, 1)
            curl.setopt(pycurl.POSTFIELDS, data)

        # Setup HTTP headers
        if request['format'] == 'HTML':
            content_type = 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        elif request['format'] == 'JSON':
            content_type = 'Accept: application/json, text/javascript, */*; q=0.01'
        elif request['format'] == 'MANIFEST':
            content_type = 'Accept: text/cache-manifest;q=0.9'
        curl.setopt(pycurl.HTTPHEADER, [content_type])

        # Setup other optiosn
        response = StringIO()
        curl.setopt(pycurl.USERAGENT, ANDROID_USER_AGENT)
        curl.setopt(pycurl.WRITEFUNCTION, response.write)
        curl.setopt(pycurl.FOLLOWLOCATION, 1)
        curl.setopt(pycurl.MAXREDIRS, 5)
        curl.setopt(pycurl.TIMEOUT, 300)
        curl.setopt(pycurl.VERBOSE, 1 if self.verbose else 0)
        try:
            curl.perform()
        except Exception, e:
            print("Failed to perform request. Reason: %s" % e)
        complete_time = datetime.now()

        log = [
            'Replayed %s "%s" on "%s" at %s' % (request['method'], request['uri'], request['host'], self._strftime(request['time'])),
            'Processed as %s' % request['format'],
            'Parameters: %s' % request['parameters'],
            'Started at %s. Completed at %s. Taken %s ms\n\n' % (self._strftime(start_time), self._strftime(complete_time), ((complete_time - start_time).microseconds / 1000)),
        ]
        print '\n'.join(log)

    def _create_timer(self, interval, requests, concurrency, speed):
        return Timer(interval / speed, self.send_requests, [requests * concurrency])

    def _flatten_list(self, timer_list):
        return [item for sublist in timer_list for item in sublist]
    
    def _strftime(self, datetime):
        return datetime.strftime('%Y-%m-%d %H:%M:%S')

if __name__ == '__main__':
    p = OptionParser()
    p.add_option('-H', '--host', help='Host server to send requests to')
    p.add_option('-l', '--log', help='Rails log file includes captured HTTP requests')
    p.add_option('-c', '--concurrency', default=1,
                 help='Number of multiple requests to replay at a time. Default: 1')
    p.add_option('-s', '--speed', default=1.0,
                 help='Ratio of request intervals according to realtime. Default: 1.0')
    p.add_option('-r', '--request', default=None,
                 help='Number of total requests to replay. Default: None')
    p.add_option('-o', '--offset', default=0,
                 help='Number of requests to start from. Default: 1')
    p.add_option('-t', '--test', action='store_true', help='Run doctest')
    opts, args = p.parse_args()

    if opts.test:
        import  doctest
        doctest.testmod()
        sys.exit(1)

    log = open(opts.log).read()
    request_logs = log.split('\n\n\n')
    if opts.request:
        request_logs = request_logs[int(opts.offset):int(opts.offset) + int(opts.request)]
    else:
        request_logs = request_logs[int(opts.offset):]
    replayer = RequestLogReplayer()
    requests = replayer.parse_log(request_logs, opts.host)
    replayer.replay_requests(requests, int(opts.concurrency), float(opts.speed))
