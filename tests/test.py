#!/usr/bin/env python3
"""
usage:
    test.py 991fad18c511a0f0032103022 K001MZBacL1ZMs6JvjtyabN9/JuTQzg
Provide your application_key_id and application_key to test.py on the command line and then check to see if
your account info stored in your Redis database correctly.

Actual unit tests coming soon.

"""
import b2sdk.v1 as b2
from b2sdk_redis import RedisAccountInfo
import sys


def main(app_key_id, secret_key, redis_host="localhost"):
    info = RedisAccountInfo(host=redis_host)
    # cache=info makes the API use Redis as the cache for the bucket name to ID map
    b2api = b2.B2Api(info, cache=info)
    b2api.authorize_account(
        realm="production",
        application_key_id=app_key_id,
        application_key=secret_key
    )
    for b in b2api.list_buckets():
        print(b)


if __name__ == "__main__":
    main(*sys.argv[1:])
