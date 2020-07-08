import b2sdk.v1 as b2
import b2sdk.v1.exception as exc
import json
import logging
import redis
import six


logger = logging.getLogger(__name__)


class RedisAccountInfo(b2.UrlPoolAccountInfo, b2.AbstractCache):
    """
    Store account information in a Redis database which is used to manage concurrent access to the data.
    """
    ACCOUNT_ID_KEY = "account-id"
    APPLICATION_KEY_ID_KEY = "application-key-id"
    ALLOWED_KEY = "allowed"
    API_URL_KEY = "api-url"
    APPLICATION_KEY_KEY = "application-key"
    AUTH_TOKEN_KEY = "auth-token"
    BUCKET_MAP_KEY = "bucket-map"
    DOWNLOAD_URL_KEY = "download-url"
    MIN_PART_SIZE_KEY = "min-part-size"
    REALM_KEY = "realm"

    ALL_KEYS = (
        ACCOUNT_ID_KEY,
        APPLICATION_KEY_ID_KEY,
        ALLOWED_KEY,
        API_URL_KEY,
        APPLICATION_KEY_KEY,
        AUTH_TOKEN_KEY,
        BUCKET_MAP_KEY,
        DOWNLOAD_URL_KEY,
        MIN_PART_SIZE_KEY,
        REALM_KEY,
    )
    """Every possible key name we can insert into Redis so we can selectively delete them later. These keys are 
    prepended with an instance's self.key_prefix first."""

    def __init__(self, key_prefix='b2sdk:', host='localhost', port=6379, db=0, *args, **kwargs):
        """
        Please see redis-py documentation for parameters for StrictRedis (or Redis) client.

        :param key_prefix: string to prepend to all keys we insert into Redis
        :type key_prefix: str
        :param host: Redis server hostname or IP
        :type host: str
        :param port: Redis server port to connect to
        :type port: int
        :param db: which db index to use (defaults to 0)
        :type db: int
        :param args: positional arguments to pass to redis.StrictRedis
        :param kwargs: keyword arguments to pass to redis.StrictRedis
        """
        super(RedisAccountInfo, self).__init__()

        kwargs["decode_responses"] = True
        self._redis = redis.StrictRedis(host, port, db, *args, **kwargs)
        self._key_prefix = None
        self.key_prefix = key_prefix

    @property
    def account_id(self):
        return self.get_account_id()

    @property
    def _account_id_key(self):
        return "%s%s" % (self.key_prefix, self.ACCOUNT_ID_KEY)

    @property
    def _all_keys(self):
        return ["%s%s" % (self.key_prefix, key) for key in self.ALL_KEYS]

    @property
    def allowed(self):
        return self.get_allowed()

    @property
    def _allowed_key(self):
        return "%s%s" % (self.key_prefix, self.ALLOWED_KEY)

    @property
    def applcation_key_id(self):
        return self.get_application_key_id()

    @property
    def _application_key_id_key(self):
        return "%s%s" % (self.key_prefix, self.APPLICATION_KEY_ID_KEY)

    @property
    def application_key(self):
        return self.get_application_key()

    @property
    def _application_key_key(self):
        return "%s%s" % (self.key_prefix, self.APPLICATION_KEY_KEY)

    @property
    def auth_token(self):
        return self.get_account_auth_token()

    @property
    def _auth_token_key(self):
        return "%s%s" % (self.key_prefix, self.AUTH_TOKEN_KEY)

    @property
    def api_url(self):
        return self.get_api_url()

    @property
    def _api_url_key(self):
        return "%s%s" % (self.key_prefix, self.API_URL_KEY)

    @property
    def _bucket_map(self):
        return self._redis.hgetall(self.bucket_map_key)

    @property
    def bucket_map_key(self):
        return "%s%s" % (self.key_prefix, self.BUCKET_MAP_KEY)

    @property
    def download_url(self):
        return self.get_download_url()

    @property
    def _download_url_key(self):
        return "%s%s" % (self.key_prefix, self.DOWNLOAD_URL_KEY)

    @property
    def key_prefix(self):
        return self._key_prefix

    @key_prefix.setter
    def key_prefix(self, value):
        value = six.ensure_str(value)
        self._key_prefix = value

    @property
    def minimum_part_size(self):
        return self.get_minimum_part_size()

    @property
    def _minimum_part_size_key(self):
        return "%s%s" % (self.key_prefix, self.MIN_PART_SIZE_KEY)

    @property
    def realm(self):
        return self.get_realm()

    @property
    def _realm_key(self):
        return "%s%s" % (self.key_prefix, self.REALM_KEY)

    def clear(self):
        """
        Remove all stored information.
        """
        self._redis.delete(*self._all_keys)
        # UrlPoolAccountInfo also calls _reset_upload_pools()
        return super(RedisAccountInfo, self).clear()

    def refresh_entire_bucket_name_cache(self, name_id_iterable):
        """
        Remove all previous name-to-id mappings and stores new ones.

        :param name_id_iterable: iterable of tuples like [(bucket_name, bucket_id), (bucket_name, bucket_id)]
        :type name_id_iterable: collections.Iterable[(str, str)]
        """
        new_map = {bucket_name: bucket_id for bucket_name, bucket_id in name_id_iterable}
        pipeline = self._redis.pipeline()
        pipeline.delete(self.bucket_map_key)
        if new_map:  # empty dict will cause an error if passed into hset
            pipeline.hset(self.bucket_map_key, mapping=new_map)
        pipeline.execute()

    def remove_bucket_name(self, bucket_name):
        """
        Remove one entry from the bucket name cache.

        :param bucket_name: the name of the bucket to eject from the cache
        :type bucket_name: str
        :return: 1 if the bucket name was removed, 0 if no bucket by that name existed
        :rtype: int
        """
        return self._redis.hdel(self.bucket_map_key, bucket_name)

    def save_bucket(self, bucket):
        """
        Remember the ID for the given bucket name.

        :param bucket: the bucket to remember the name to ID for
        :type bucket: b2sdk.v1.Bucket
        :return: 1 if the bucket name was added, 0 if that bucket name existed (updated if different)
        :rtype: int
        """
        return self._redis.hset(self.bucket_map_key, bucket.name, bucket.id_)

    def get_bucket_id_or_none_from_bucket_name(self, bucket_name=None, name=None):
        """
        AbstractCache's parameter is name. AbstractAccountInfo's parameter is bucket_name. They do the same thing.
        Hence both are accepted here. Some extra logic checks to make sure that only one was provided, or if
        both were provided, there's no problem as long as they are the same.

        :param bucket_name: the bucket name to get an ID for (equivalent to the name param)
        :type bucket_name str
        :param name: the bucket name to get an ID for (equivalent to the bucket_name param)
        :type name str
        :return: the corresponding bucket_id if found in cache, otherwise None
        :rtype str|None
        """
        # if user provided both parameters and they're two different values
        if name != bucket_name and None not in (bucket_name, name):
            raise ValueError("Got '{name}' and '{bucket_name}' in get_bucket_id_or_none_from_bucket_name. "
                             "Please specify one or the other.".format(name=name, bucket_name=bucket_name))
        bucket_name = bucket_name or name  # coalesce the values into just the one bucket_name variable
        try:
            bucket_name = six.ensure_str(bucket_name)
        except TypeError:
            raise ValueError("'{}' is not a valid bucket_name.".format(bucket_name))

        bucket_id = self._redis.hget(self.bucket_map_key, bucket_name)
        return bucket_id

    def get_account_id(self):
        """
        Return account ID or raises MissingAccountData exception.
        """
        return self._get_account_info_or_raise(self._account_id_key)

    def get_application_key_id(self):
        """
        Return the application key ID used to authenticate.
        """
        return self._get_account_info_or_raise(self._application_key_id_key)

    def get_account_auth_token(self):
        return self._get_account_info_or_raise(self._auth_token_key)

    def get_bucket_name_or_none_from_allowed(self):
        allowed = self.get_allowed()
        return allowed.get("bucketName")

    def get_api_url(self):
        return self._redis.get(self._api_url_key)

    def get_application_key(self):
        return self._get_account_info_or_raise(self._application_key_key)

    def get_download_url(self):
        return self._get_account_info_or_raise(self._download_url_key)

    def get_realm(self):
        return self._get_account_info_or_raise(self._realm_key)

    def get_minimum_part_size(self):
        return int(self._get_account_info_or_raise(self._minimum_part_size_key))

    def get_allowed(self):
        allowed_json = self._get_account_info_or_raise(self._allowed_key)
        if allowed_json is None:
            return self.DEFAULT_ALLOWED
        else:
            return json.loads(allowed_json)

    def set_bucket_name_cache(self, buckets):
        bucket_iter = self._name_id_iterator(buckets)
        self.refresh_entire_bucket_name_cache(bucket_iter)

    def _get_account_info_or_raise(self, column_name):
        """
        Return the stored string value for requested account parameter. If it is not stored,
        raise MissingAccountData exception rather than returning None.
        :param column_name: the account info field to get the value of
        :type column_name: str
        :return: the value of the requested account info field.
        :rtype: str
        :raises b2sdk.account_info.exception.MissingAccountData: if the value for that field was not stored
        """
        result = self._redis.get(column_name)
        if result is None:
            raise exc.MissingAccountData("`%s` not found." % column_name)

        return result

    def _set_auth_data(self, account_id, auth_token, api_url, download_url, minimum_part_size, application_key, realm,
                       allowed, application_key_id):
        """
        Actually store the auth data.  Can assume that 'allowed' is present and valid.

        All of the information returned by ``b2_authorize_account`` is saved, because all of it is
        needed at some point.

        See the Backblaze B2 API docs for more information on each parameter:
        https://www.backblaze.com/b2/docs/b2_authorize_account.html

        :param account_id: The identifier for the account.
        :type account_id: str
        :param auth_token: An authorization token, valid for at most 24 hours, to use with all calls.
        :type auth_token: str
        :param api_url: The base URL to use for all API calls except for uploading and downloading files.
        :type api_url: str
        :param download_url: The base URL to use for downloading files.
        :type download_url: str
        :param minimum_part_size: The recommended size for each part of a large file. (recommendedPartSize)
        :type minimum_part_size: int
        :param application_key: the secret portion of the application_key we authorized with
        :type application_key: str
        :param realm: this is "production" unless maybe you are a Backblaze employee
        :type realm: str
        :param allowed: A dict containing the capabilities of this auth token, and any restrictions on using it.
        :type allowed: dict[str,str|list]
        :param application_key_id: the identifier of the application_key we authorized with
        :type application_key_id: str
        """
        self._redis.mset({
            self._account_id_key: account_id,
            self._application_key_id_key: application_key_id,
            self._application_key_key: application_key,
            self._auth_token_key: auth_token,
            self._api_url_key: api_url,
            self._download_url_key: download_url,
            self._minimum_part_size_key: minimum_part_size,
            self._realm_key: realm,
            self._allowed_key: json.dumps(allowed),
        })
