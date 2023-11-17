import urllib


class ODataUrl:
    def __init__(self, base_url):
        self.base_url = base_url

    def parse(self, uri="/", params={}):
        url = urllib.parse.urljoin(self.base_url, uri)

        if params:
            query = "?"
            pl = len(params)
            for idx, (k, v) in enumerate(params.items()):
                query += f"{k}={v}"
                if idx != (pl - 1):
                    query += "&"

            url += query
        return url

    def count(self, uri="/"):
        url = urllib.parse.urljoin(self.base_url, uri)
        url += "/$count"
        return url
