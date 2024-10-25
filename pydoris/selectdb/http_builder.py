import base64
import requests


class HttpPostBuilder:
    def __init__(self):
        self.url = None
        self.headers = {}
        self.http_entity = None

    def set_url(self, url):
        self.url = url
        return self

    def add_common_header(self):
        self.headers['Expect'] = '100-continue'
        return self

    def base_auth(self, user, password):
        auth_info = f"{user}:{password}"
        encoded = base64.b64encode(auth_info.encode('utf-8')).decode('utf-8')
        self.headers['Authorization'] = f"Basic {encoded}"
        return self

    def set_entity(self, http_entity):
        self.http_entity = http_entity
        return self

    def build(self):
        if not self.url or not self.http_entity:
            raise ValueError("URL and HTTP entity must be set")

        post_request = requests.Request('POST', self.url, headers=self.headers, data=self.http_entity)
        return post_request


class HttpPutBuilder:
    def __init__(self):
        self.url = None
        self.headers = {}
        self.http_entity = None

    def set_url(self, url):
        self.url = url
        return self

    def add_file_name(self, file_name):
        self.headers['fileName'] = file_name
        return self

    def set_empty_entity(self):
        self.http_entity = ""
        return self

    def add_common_header(self):
        self.headers['Expect'] = '100-continue'
        return self

    def base_auth(self, user, password):
        auth_info = f"{user}:{password}"
        encoded = base64.b64encode(auth_info.encode('utf-8')).decode('utf-8')
        self.headers['Authorization'] = f"Basic {encoded}"
        return self

    def set_entity(self, http_entity):
        self.http_entity = http_entity
        return self

    def build(self):
        if not self.url:
            raise ValueError("URL must be set")

        put_request = requests.Request('PUT', self.url, headers=self.headers, data=self.http_entity)
        return put_request