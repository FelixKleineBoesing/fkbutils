import logging
import traceback
import redis
from typing import Union, List
from types import FunctionType, MethodType
import json
import datetime
import time
from multiprocessing import Process
from threading import Thread


class RedisWrapper:
    """
    RedisWrapper supports for now working with:
     - queues
     - key-value-store
     - channels
    """
    def __init__(self, host: str, port: int, db: int, password: str = None):
        assert isinstance(host, str), "Host must be of type string"
        assert isinstance(port, int), "port must be of type integer"
        assert isinstance(db, int), "db must be of type integer"
        if password is not None:
            assert isinstance(password, str), "password must be of type string"

        self.host = host
        self.port = port
        self.password = password
        if password is not None:
            self.conn = redis.StrictRedis(host=host, port=port, db=db, password=password)
        else:
            self.conn = redis.StrictRedis(host=host, port=port, db=db)

    def put_on_queue(self, queue: str, value: Union[list, dict]):
        """
        puts a task to redis que

        :param queue:
        :param value:
        :return:
        """
        self.conn.rpush("queue:{}".format(queue), json.dumps(value))

    def length_of_queue(self, queue: str):
        """
        gets the length of the given queue

        :param queue: queue name
        :return:
        """
        self.conn.llen("queue:{}".format(queue))

    def get_from_queue(self, queue: str, timeout: int = 1):
        """
        returns a value if there is one in queue, otherwise returns None if there is no value after the specified
        timeout

        :param queue: the name of the queue from which a value should be taken
        :param timeout: timeout in seconds
        :return:
        """
        value = self.conn.blpop(["queue:{}".format(queue)], timeout=timeout)
        return value

    def set_value_by_key(self, key: str, value):
        """
        puts any possible value by key into redis

        :param key: key with which the data will be put into redis
        :param value: serializiable value
        :return:
        """
        assert isinstance(key, str), "key must be of type string"
        try:
            self.conn.set(name=key, value=value)
        except Exception as e:
            logging.error("Setting value in redis failed with exceotion: {}! Most probably the value is "
                          "not serializble".format(e))
            logging.error(traceback.format_exc())
            raise e

    def get_value_by_key(self, key: str):
        """
        gets a value from redis in case the key is existing in redis. Otherwise raises an KeyError!

        :param key: key which must exist in redis
        :return:
        """
        assert isinstance(key, str), "key must be of type string"
        if not self.exists(key):
            raise KeyError("Key is not present in Redis! Value can´t be called!")
        try:
            value = self.conn.get(key)
        except Exception as e:
            logging.error("Getting value in redis failed with exceotion: {}! ".format(e))
            logging.error(traceback.format_exc())
            raise e
        return value

    def exists(self, keys: Union[str, List[str]]) -> int:
        """
        checks if a key exists in redis

        :param keys: a single string or a list of strings
        :return: the number of keys that are present in redis
        """
        assert isinstance(keys, str) or isinstance(keys, list)
        if isinstance(keys, list):
            assert all([isinstance(key, str) for key in keys])

        return self.conn.exists(keys)

    def publish_to_channel(self, channel: str, value):
        """
        publishes a value to the specified channel

        :param channel:
        :param value:
        :return:
        """
        assert isinstance(channel, str)
        self.conn.publish(channel=channel, message=value)

    def create_subscribe_object(self, channel: str):
        """
        returns a publish/subscribe obbject with which you are able to get_messages or publish messages
        call the method get_message on this object to get get a published message

        example:
            import time
            sub_object = redis.create_subscribe_object()
            while True:
                message = sub_object.get_message()
                if message:
                    do_things(messgge)
                else:
                    time.sleep(1)

        :param channel: the channel which you want to subscribe
        :return:
        """
        assert isinstance(channel, str)
        p = self.conn.pubsub()
        p.subscribe(channel)
        return p

    def subscribe_to_channel(self, channel: str, callback_function: Union[MethodType, FunctionType] = None,
                             sync: bool = True, method: str = "process",
                             timeout: int = None):
        """
        subscribes to a channel and executes the callback_function in case there is a message published.

        :param channel: the name of the channel which should be subscribed
        :param callback_function: function that should be executed when a message is published. The function gets a
            argument called message. The callback_function should return whether the loop should be continues (True)
            or not (False). If you want to get data back from this function, wrap the callback_function with another
            function and append the data to a dictionary that you are creating beforehand. An alternative is to supply
            the method of a class instance and the data is stored as an instance attribute
        :param sync: if False the while loop will be started in a new process/thread to unlock the main process again
            Take care that your callback_function is able to supply the data back even if the task will be launched in
            a new process
        :param method: if sync is false this parameter will be used to spawn the task in either a process or thread.
            Therefore the method can be "process" or "thread"
        :param timeout: If you want, you can definae a timeout in seconds after which the subscription should be
            cancelled
        :return:
        """
        assert isinstance(channel, str)
        if callback_function is not None:
            assert isinstance(callback_function, FunctionType) or isinstance(callback_function, MethodType)
        else:
            def callback_function(message):
                if not hasattr(callback_function, "i"):
                    callback_function.i = 0
                print(message)
                callback_function.i += 1
                if callback_function.i >= 10:
                    return False
                else:
                    return True

        assert isinstance(sync, bool)
        assert isinstance(method, str)
        if timeout is not None:
            assert isinstance(timeout, int)

        self._start_subscription(channel, callback_function, timeout, sync, method)

    def _start_subscription(self, channel: str, callback_function, timeout, sync, method):
        """
        starts subscription for the given channel

        :param channel: channel for which the subscription should be started
        :param callback_function:
        :param timeout:
        :param sync:
        :param method:
        :return:
        """
        def subscription_while_task():
            sub_object = self.create_subscribe_object(channel)
            start = datetime.datetime.now()

            while True:
                msg = sub_object.get_message()

                if msg:
                    continue_ = callback_function(msg)
                    if not continue_:
                        break

                if timeout is not None:
                    duration = (datetime.datetime.now() - start).seconds
                    if duration > timeout:
                        break

                time.sleep(1)

        if sync:
            subscription_while_task()
        else:
            parallel_class = Process if method == "process" else Thread
            pc = parallel_class(target=subscription_while_task)
            pc.start()
