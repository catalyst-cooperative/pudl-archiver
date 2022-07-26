"""Mocking fixtures for the Scrapy tests."""
from pathlib import Path

import factory
from scrapy.http import Request, TextResponse

BASE_PATH = Path(__file__).parent


class RequestFactory(factory.Factory):
    """Factory to generate fake requests."""
    class Meta:
        """Metadata class."""
        model = Request

    url = "http://example.com"

    @factory.post_generation
    def meta(self, create, extracted, **kwargs):
        """Generate some meta-metadata."""
        if extracted is not None:
            for k, v in extracted.items():
                self.meta[k] = v


def test_path(filename):
    """Path to the testing data file."""
    return BASE_PATH / "data" / filename


class FakeResponse(TextResponse):
    """Fake a response for spider testing."""
    def __init__(self, url, file_path, *args, **kwargs):
        """Initialize the fake response."""
        with open(file_path, "rb") as f:
            contents = f.read()

        super().__init__(url, *args, body=contents, **kwargs)


class TestResponseFactory(factory.Factory):
    """Factory for generating server responses during testing."""
    class Meta:
        """Metdata class."""
        model = FakeResponse
        inline_args = ("url", "file_path")

    class Params:
        """Parameters to use in generating responses for each test case."""
        eia860 = factory.Trait(
            url="https://www.eia.gov/electricity/data/eia860/",
            file_path=test_path("eia860.html"),
        )

        eia860m = factory.Trait(
            url="https://www.eia.gov/electricity/data/eia860m/",
            file_path=test_path("eia860m.html"),
        )

        eia861 = factory.Trait(
            url="https://www.eia.gov/electricity/data/eia861/",
            file_path=test_path("eia861.html"),
        )

        eia923 = factory.Trait(
            url="https://www.eia.gov/electricity/data/eia923/",
            file_path=test_path("eia923.html"),
        )

        ferc1 = factory.Trait(
            url="https://www.ferc.gov/docs-filing/forms/form-1/data.asp",
            file_path=test_path("ferc1.html"),
        )

    encoding = "utf-8"
    request = factory.SubFactory(RequestFactory, url=factory.SelfAttribute("..url"))
