import pycurl
from io import BytesIO
#from io import StringIO
#import certifi
import chardet
from collections import OrderedDict
from collections.abc import MutableMapping, Mapping
import json as json_
import re

from threading import Thread, Lock


class CaseInsensitiveDict(MutableMapping):
    """A case-insensitive ``dict``-like object.
    Implements all methods and operations of
    ``MutableMapping`` as well as dict's ``copy``. Also
    provides ``lower_items``.
    All keys are expected to be strings. The structure remembers the
    case of the last key to be set, and ``iter(instance)``,
    ``keys()``, ``items()``, ``iterkeys()``, and ``iteritems()``
    will contain case-sensitive keys. However, querying and contains
    testing is case insensitive::
        cid = CaseInsensitiveDict()
        cid['Accept'] = 'application/json'
        cid['aCCEPT'] == 'application/json'  # True
        list(cid) == ['Accept']  # True
    For example, ``headers['content-encoding']`` will return the
    value of a ``'Content-Encoding'`` response header, regardless
    of how the header name was originally stored.
    If the constructor, ``.update``, or equality comparison
    operations are given keys that have equal ``.lower()``s, the
    behavior is undefined.
    """

    def __init__(self, data=None, **kwargs):
        self._store = OrderedDict()
        if data is None:
            data = {}
        self.update(data, **kwargs)

    def __setitem__(self, key, value):
        # Use the lowercased key for lookups, but store the actual
        # key alongside the value.
        self._store[key.lower()] = (key, value)

    def __getitem__(self, key):
        return self._store[key.lower()][1]

    def __delitem__(self, key):
        del self._store[key.lower()]

    def __iter__(self):
        return (casedkey for casedkey, mappedvalue in self._store.values())

    def __len__(self):
        return len(self._store)

    def lower_items(self):
        """Like iteritems(), but with all lowercase keys."""
        return (
            (lowerkey, keyval[1])
            for (lowerkey, keyval)
            in self._store.items()
        )

    def __eq__(self, other):
        if isinstance(other, Mapping):
            other = CaseInsensitiveDict(other)
        else:
            return NotImplemented
        # Compare insensitively
        return dict(self.lower_items()) == dict(other.lower_items())

    # Copy is required
    def copy(self):
        return CaseInsensitiveDict(self._store.values())

    def __repr__(self):
        return str(dict(self.items()))


class LookupDict(dict):
    """Dictionary lookup object."""

    def __init__(self, name=None):
        self.name = name
        super(LookupDict, self).__init__()

    def __repr__(self):
        return '<lookup \'%s\'>' % (self.name)

    def __getitem__(self, key):
        # We allow fall-through here, so values default to None

        return self.__dict__.get(key, None)

    def get(self, key, default=None):
        return self.__dict__.get(key, default)


class Response(object):
    def __init__(self):
        self.request = None  # type: Optional[Request]
        self.elapsed = None  # type: Optional[datetime.timedelta]
        self.status_code = None  # type: Optional[int]
        self.reason = None  # type: Optional[str]
        self.headers = None  # type: Optional[structures.CaseInsensitiveDict]
        self.encoding = None  # type: Optional[str]
        self.url = None  # type: Optional[str]
        self.raw = None  # type: Optional[BytesIO]

    @property
    def apparent_encoding(self):
        return chardet.detect(self.content)['encoding']

    def close(self):
        # Not implemented
        pass

    @property
    def content(self):
        return self.raw.getvalue()

    @property
    def cookies(self):
        return NotImplemented

    @property
    def history(self):
        return NotImplemented

    @property
    def is_permanent_redirect(self):
        # Moved Permanently (HTTP 301) or Permanent Redirect (HTTP 308)
        return self.status_code in {301, 308}

    @property
    def is_redirect(self):
        return self.status_code in {301, 302, 303, 307, 308}

    def iter_content(self, chunk_size=1, decode_unicode=False):
        chunk_size = chunk_size or -1
        decoder = codecs.getincrementaldecoder(self.encoding)('replace') if self.encoding and decode_unicode else None
        for chunk in iter(lambda: self.raw.read1(chunk_size), b''):
            if decoder:
                yield decoder.decode(chunk)
            else:
                yield chunk

        if decoder:
            # Make sure we finalize the decoder (may yield replacement character)
            tail = decoder.decode(b'', True)
            if tail:
                yield tail

    def iter_lines(self, chunk_size=512, decode_unicode=False, delimiter=None):
        leftover = None
        for chunk in self.iter_content(chunk_size, decode_unicode=decode_unicode):
            if leftover:
                chunk = leftover + chunk

            if delimiter is not None:
                parts = chunk.split(delimiter)
            else:
                parts = chunk.splitlines()

            # FIXME: This logic doesn't work for CR-only line endings
            if chr(ord(chunk[-1])) == '\n':
                yield from parts
                leftover = None
            else:
                # This may be a partial line, so add to the next chunk
                yield from parts[:-1]
                leftover = parts[-1]

        if leftover is not None:
            yield leftover

    def json(self, **kwargs):
        return json_.loads(self.content, **kwargs)

    @property
    def links(self):
        return NotImplemented

    @property
    def next(self):
        return NotImplemented

    @property
    def ok(self):
        return self.status_code < 400

    def raise_for_status(self):
        if 400 <= self.status_code < 500:
            raise exceptions.HTTPError('{s.status_code} Client Error: {s.reason} for url: {s.url}'.format(s=self),
                                       response=self)

        if 500 <= self.status_code < 600:
            raise exceptions.HTTPError('{s.status_code} Client Error: {s.reason} for url: {s.url}'.format(s=self),
                                       response=self)

    @property
    def text(self):
        return self.content.decode(self.encoding or 'ISO-8859-1')


class RequestPool(object):
    def __init__(self):
        self.mutex = Lock()
        self.request_pool = []
        self.work_pool = []
        self.multi = pycurl.CurlMulti()

    # def __del__(self):
    # def __exit__(self):
    def putRequest(self, req):
        self.request_pool.append(req)

    def getRequest(self):
        i = 0
        while len(self.request_pool) < 1:
            i = 1 + 1
        self.mutex.acquire()
        req = self.request_pool[0]
        self.work_pool.append(req)
        self.request_pool.remove(req)
        self.mutex.release()
        return req

    def freeRequest(self, req):
        self.mutex.acquire()
        self.work_pool.remove(req)
        self.request_pool.insert(0, req)
        self.mutex.release()


class Request(object):
    def __init__(self, requestPool):
        self.request_pool = requestPool;
        self.request_headers = None  # CaseInsensitiveDict()
        self.encoding = None
        self.url = None
        self.status_code = None
        self.response_buffer = BytesIO()
        self.method = None
        self.request_data = None
        self.timeout = None
        self.url = None
        self.curl = pycurl.Curl()
        self.response_headers = None
        self.curl.setopt(pycurl.SSL_VERIFYPEER, False)
        self.curl.setopt(pycurl.WRITEDATA, self.response_buffer)
        # self.curl.setopt(pycurl.DNS_SERVERS, '8.8.8.8')
        # self.request_pool.multi.add_handle(self.curl)

    # def __del__(self):
    def __exit__(self):
        self.response_buffer.close()
        self.curl.close()

    def header_function(self, header_line):
        header_line = header_line.decode('iso-8859-1')
        if ':' not in header_line:
            return
        name, value = header_line.split(':', 1)
        name = name.strip()
        value = value.strip()
        name = name.lower()
        self.response_headers[name] = value

    def build_response(self, elapsed=None):
        status_code = self.curl.getinfo(pycurl.RESPONSE_CODE)
        if not status_code:
            return None

        response = Response()
        response.status_code = status_code
        response.url = self.url
        response.raw = self.response_buffer
        return response

    def send(self, method, url, headers=None, data=None, timeout=None):

        self.method = method
        self.url = url
        self.request_headers = headers
        self.data = data
        self.timeout = timeout

        hds = []
        if self.request_headers is not None:
            hds = ['{}: {}'.format(n, v.decode('utf-8') if isinstance(v, bytes) else v) for n, v in
                   self.request_headers.items()];

        # if self.method.lower()=="post":
        self.curl.setopt(pycurl.CUSTOMREQUEST, self.method.upper())
        hds.append('Accept-Encoding: utf-8')
        hds.append('Connection: keep-alive')

        if self.data is None or len(self.data) == 0:
            self.curl.setopt(pycurl.POSTFIELDSIZE, 0)
            hds.append('Content-Length: 0')
            hds.append('Content-Type: ')
        else:
            self.curl.setopt(pycurl.POST, 1)
            postdata = None
            if isinstance(self.data, str):
                postdata = self.data
                hds.append('Content-Type: application/json')
            elif isinstance(data, bytes):
                postdata = json_.dumps(self.data)
                hds.append('Content-Type: application/json')
            elif isinstance(data, list):
                postdata = ""
                hds.append('Content-Type: application/x-www-form-urlencoded')
                for item in data:
                    postdata = postdata + str(item[0]) + "=" + str(item[1]) + "&"
                postdata = postdata[:-1]
            self.curl.setopt(pycurl.POSTFIELDS, postdata)
            self.curl.setopt(pycurl.POSTFIELDSIZE, len(postdata))

        if self.timeout is not None:
            self.curl.setopt(pycurl.CONNECTTIMEOUT, self.timeout)

        self.curl.setopt(pycurl.HTTPHEADER, hds)
        self.curl.setopt(pycurl.VERBOSE, 0)
        self.curl.setopt(pycurl.URL, self.url)
        self.response_buffer.seek(0)
        self.response_buffer.truncate(0)
        self.curl.perform()
        response = self.build_response()

        self.request_pool.freeRequest(self)
        return response


class Session(object):
    def __init__(self, num=0):
        if num == 0:
            num = 100
        self.request_pool = RequestPool()
        for i in range(0, num - 1):
            self.request_pool.putRequest(Request(self.request_pool))
        self.headers = {}
        self.check_response = True

    # def __del__(self):
    # def __exit__(self):
    def request(self, method, url, headers=None, data=None, timeout=None):

        if headers is not None:
            self.headers = headers

        req = self.request_pool.getRequest()
        response = req.send(method, url, self.headers, data, timeout)
        return response

    def get(self, url, headers=None, params=None, timeout=None):
        if headers is not None:
            self.headers = headers
        req = self.request_pool.getRequest()
        if params is not None:
            url = url + "?" + params;
        response = req.send("GET", url, self.headers, None, timeout)
        if self.check_response:
            assert (response.status_code != 401), url + " :GET ERROR RESPONSE : " + str(response.content)
        return response

    def post(self, url, headers=None, data=None, timeout=None):
        if headers is not None:
            self.headers = headers
        req = self.request_pool.getRequest()
        response = req.send("POST", url, self.headers, data, timeout)
        if self.check_response:
            assert (response.status_code != 401), url + " :POST ERROR RESPONSE : " + str(response.content)
        return response

    def put(self, url, headers=None, data=None, timeout=None):
        if headers is not None:
            self.headers = headers
        req = self.request_pool.getRequest()
        response = req.send("PUT", url, self.headers, data, timeout)
        if self.check_response:
            assert (response.status_code != 401), url + " :PUT ERROR RESPONSE : " + str(response.content)
        return response

    def delete(self, url, headers=None, data=None, timeout=None):
        if headers is not None:
            self.headers = headers
        req = self.request_pool.getRequest()
        response = req.send("DELETE", url, self.headers, data, timeout)
        if self.check_response:
            assert (response.status_code != 401), url + " :DELETE ERROR RESPONSE : " + str(response.content)
        return response


def session():
    return Session()



