# RedisAccountInfo for b2sdk
Use Redis to cache your b2-sdk-python account info!

## What It Is
The official B2 Python SDK (b2sdk) uses AccountInfo objects to store your credentials, authorization tokens, and bucket name to ID mappings. Only SqliteAccountInfo and InMemoryAccountInfo are provided by default.

**RedisAccountInfo:**
  - Implements `b2sdk.account_info.abstract.AbstractAccountInfo`
  - Implements `b2sdk.cache.AbstractCache`
  - Operations are atomic and transactional. Safe for multithreaded and multiprocess applications!

## Example
```py
import b2sdk.v1 as b2
from b2sdk_redis import RedisAccountInfo

info = RedisAccountInfo(host="localhost")
b2api = b2.B2Api(info, cache=info)  # use cache=info to cache bucket name to ID mappings
b2api.authorize_account(
    realm="production", 
    application_key_id=app_key_id, 
    application_key=secret_key
)
for b in b2api.list_buckets():
    print(b)
```

## Alpha!
  - Hasn't been rigorously tested
  - Safety not guaranteed

## License
MIT
