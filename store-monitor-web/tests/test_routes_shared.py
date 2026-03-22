from routes._shared import _build_url


def test_build_url_encodes_query_values():
    url = _build_url("/tasks/group/1", error="a/b & c", target_name="A&B/新区")

    assert url == "/tasks/group/1?error=a%2Fb+%26+c&target_name=A%26B%2F%E6%96%B0%E5%8C%BA"
