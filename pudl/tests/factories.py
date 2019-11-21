# -*- coding: utf-8 -*-
import os
import factory
from scrapy.http import Request, TextResponse

BASE_PATH = os.path.dirname(os.path.abspath(__file__))


class RequestFactory(factory.Factory):
    class Meta:
        model = Request

    url = "http://example.com"

    @factory.post_generation
    def meta(obj, create, extracted, **kwargs):
        if extracted is not None:
            for k, v in extracted.items():
                obj.meta[k] = v


def test_path(filename):
    return os.path.join(BASE_PATH, "data", filename)


class FakeResponse(TextResponse):
    """Fake a response for spider testing"""

    def __init__(self, url, file_path, *args, **kwargs):

        with open(file_path, "rb") as f:
            contents = f.read()

        super().__init__(url, *args, body=contents, **kwargs)


class TestResponseFactory(factory.Factory):
    class Meta:
        model = FakeResponse
        inline_args = ("url", "file_path")

    class Params:
        eia860 = factory.Trait(
            url="https://www.eia.gov/electricity/data/eia860/",
            file_path=test_path("eia860.html"))

        eia923 = factory.Trait(
            url="https://www.eia.gov/electricity/data/eia923/",
            file_path=test_path("eia923.html"))

    encoding = "utf-8"
    request = factory.SubFactory(
        RequestFactory, url=factory.SelfAttribute("..url"))
