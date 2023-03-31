import itertools
import json
import os
from collections import defaultdict

from ecmwf.opendata import Client
from ecmwf.opendata.client import PATTERNS, HOURLY_PATTERN, EXTENSIONS, Result, warning_once
from ecmwf.opendata.date import full_date
from multiurl import download, robust
import requests
import datetime


class ECWMFPatchedClient(Client):
    def download(self, request=None, target=None, timeout=None, **kwargs):
        result = self._get_urls(request, target=target, use_index=False, **kwargs)
        result.size = download(result.urls, target=result.target, timeout=timeout)
        return result

    def retrieve(self, request=None, target=None, timeout=None, **kwargs):
        result = self._get_urls(request, target=target, use_index=True, **kwargs)
        result.size = download(result.urls, target=result.target, timeout=None)
        return result

    def latest(self, request=None, timeout=None, maximum_tries=10, retry_after=120, **kwargs):
        if request is None:
            params = dict(**kwargs)
        else:
            params = dict(**request)

        if "time" not in params:
            delta = datetime.timedelta(hours=6)
        else:
            delta = datetime.timedelta(days=1)

        date = full_date(0, params.get("time", 18))

        stop = date - datetime.timedelta(days=1, hours=6)

        while date > stop:
            result = self._get_urls(
                request=None,
                use_index=False,
                date=date,
                timeout=timeout,
                maximum_tries=maximum_tries,
                retry_after=retry_after,
                **params,
            )
            codes = [robust(requests.head, maximum_tries=maximum_tries,
                            retry_after=retry_after)(url, timeout=timeout).status_code for url in result.urls]

            if len(codes) > 0 and all(c == 200 for c in codes):
                return date
            date -= delta

        raise ValueError("Cannot etablish latest date for %r" % (result.for_urls,))

    def _get_urls(self, request=None, use_index=None, target=None, timeout=None, maximum_tries=10, retry_after=120,
                  **kwargs):
        assert use_index in (True, False)
        if request is None:
            params = dict(**kwargs)
        else:
            params = dict(**request)

        if "date" not in params:
            params["date"] = self.latest(params)

        if target is None:
            target = params.pop("target", None)

        for_urls, for_index = self.prepare_request(params)

        for_urls["_url"] = [self.url]

        seen = set()
        data_urls = []

        dates = set()

        for args in (
                dict(zip(for_urls.keys(), x)) for x in itertools.product(*for_urls.values())
        ):
            pattern = PATTERNS.get(args["stream"], HOURLY_PATTERN)
            date = full_date(args.pop("date", None), args.pop("time", None))
            dates.add(date)
            args["_yyyymmdd"] = date.strftime("%Y%m%d")
            args["_H"] = date.strftime("%H")
            args["_yyyymmddHHMMSS"] = date.strftime("%Y%m%d%H%M%S")
            args["_extension"] = EXTENSIONS.get(args["type"], "grib2")
            args["_stream"] = self.patch_stream(args)

            url = pattern.format(**args)
            if url not in seen:
                data_urls.append(url)
                seen.add(url)

        if for_index and use_index:
            data_urls = self.get_parts(data_urls, for_index, timeout=timeout, maximum_tries=maximum_tries,
                                       retry_after=retry_after)

        return Result(
            urls=data_urls,
            target=target,
            dates=sorted(dates),
            for_urls=for_urls,
            for_index=for_index,
        )

    def get_parts(self, data_urls, for_index, timeout=None, maximum_tries=10, retry_after=120):

        count = len(for_index)
        result = []
        line = None

        possible_values = defaultdict(set)

        for url in data_urls:
            base, _ = os.path.splitext(url)
            index_url = f"{base}.index"
            r = robust(requests.get, maximum_tries=maximum_tries, retry_after=retry_after)(index_url, timeout=timeout)
            r.raise_for_status()

            parts = []
            for line in r.iter_lines():
                line = json.loads(line)
                matches = []
                for i, (name, values) in enumerate(for_index.items()):
                    idx = line.get(name)
                    if idx is not None:
                        possible_values[name].add(idx)
                    if idx in values:
                        if self.preserve_request_order:
                            for j, v in enumerate(values):
                                if v == idx:
                                    matches.append((i, j))
                        else:
                            matches.append(line["_offset"])

                if len(matches) == count:
                    parts.append((tuple(matches), (line["_offset"], line["_length"])))

            if parts:
                result.append((url, tuple(p[1] for p in sorted(parts))))

        for name, values in for_index.items():
            diff = set(values).difference(possible_values[name])
            for d in diff:
                warning_once(
                    "No index entries for %s=%s",
                    name,
                    d,
                    did_you_mean=(d, possible_values[name]),
                )

        if not result:
            raise ValueError("Cannot find index entries matching %r" % (for_index,))

        return result
