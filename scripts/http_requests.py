import requests


class HTTP_Requests:
    def PUT(self, url, headers, payload={}):
        return requests.request("PUT", url, headers=headers, data=payload).text

    def GET(self, url, headers, payload={}):
        return requests.request("GET", url, headers=headers, data=payload).text

    def DELETE(self, url, headers, payload={}):
        return requests.request("DELETE", url, headers=headers, data=payload).text