import pymongo
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


class MongoWrapper:
    """
    wrapper for actions with mongo database

    each method has **kwargs specified, so that you are able to overrule the database and collection strings from init
    """

    def __init__(self, host: str, port: int, user: str, password: str, database: str, collection: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.collection = collection
        self.client = MongoClient(host=host, port=port, username=user, password=password)

    def get_database(self, database: str):
        """
        gets the specified database

        :param database:

        :return:
        """
        return self.client[database]

    def get_collection(self, database: str, collection: str):
        """
        get a collection of a specific database

        :param database:
        :param collection:
        :return:
        """
        db = self.client[database]
        return db[collection]

    def get_collection_from_kwargs(self, **kwargs: dict):
        """
        if database and collection is supplied in kwargs it will be primarly used

        :param kwargs: additional arguments like "database" or "collection"

        :return:
        """
        if "collection" in kwargs:
            collection = kwargs["collection"]
        else:
            collection = self.collection
        if "database" in kwargs:
            database = kwargs["database"]
        else:
            database = self.database
        return self.get_collection(database=database, collection=collection)

    def set(self, key: str, value, **kwargs):
        """
        upserts the value with the supplied key

        :param key:
        :param value:
        :param kwargs: additional arguments like "database" or "collection"

        :return:
        """
        self._upsert(key=key, value=value, **kwargs)

    def get(self, key: str, **kwargs):
        """

        :param key:
        :return:
        """
        value = self._get_by_id(key, **kwargs)
        if value is None:
            raise KeyError("key {} is not present in collection!".format(key))
        return value

    def exists(self, key: str, **kwargs):
        """
        returns whether the supplied key exists in the database

        :return:
        """

    def delete(self, key, **kwargs):
        """

        :param key:
        :param kwargs: additional arguments like "database" or "collection"
        :return:
        """
        collection = self.get_collection_from_kwargs(**kwargs)
        collection.delete_one({"_id": key})

    def _get_by_id(self, key, **kwargs):
        """
        returns the stored value of the database

        :param key:
        :param kwargs: additional arguments like "database" or "collection"

        :return:
        """
        collection = self.get_collection_from_kwargs(**kwargs)
        doc = collection.find_one({"_id": key})
        if doc is None:
            return None
        return doc["payload"]

    def _upsert(self, key, value, **kwargs):
        """
        inserts if key not exists, otherwise updates the value in the database

        :param key:
        :param value:
        :param kwargs: additional arguments like "database" or "collection"

        :return:
        """
        collection = self.get_collection_from_kwargs(**kwargs)
        document = collection.find_one({"_id": key})
        if document is None:
            try:
                collection.insert_one({"_id": key, "payload": value})
            except DuplicateKeyError as e:
                collection.update_one({"_id": key}, {"$set": {"payload": value}})
        else:
            collection.insert_one({"_id": key, "payload": value})

    def flush(self, **kwargs):
        """
        removes all keys from a specific collection or database

        :param kwargs: additional arguments like "database" or "collection"

        :return:
        """
        if "database" in kwargs:
            if "collection" in kwargs:
                collection = self.get_collection_from_kwargs(**kwargs)
                collection.drop()
            else:
                database = self.get_database(kwargs["database"])
                database.drop()
        else:
            if "collection" in kwargs:
                collection = self.get_collection_from_kwargs(**kwargs)
                collection.drop()
            else:
                collection = self.get_collection_from_kwargs()
                collection.drop()
