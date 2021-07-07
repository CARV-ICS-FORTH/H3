H3Controllers in python3
===========================

Build-in controllers to manage objects according to metadata (in this case ``ExpiresAt`` and ``ReadOnlyAfter``).

Run
------------

python3 expiresAtController --storage "file:///tmp/h3"

python3 readOnlyAfterController --storage "file:///tmp/h3"

python3 cacheController --hot_storage="{hot_storage_url}" --cold_storage="{cold_storage_url}"
