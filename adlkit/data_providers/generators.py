import Queue
import logging as lg
import time

from .config import GENERATOR_OFFSET
from .workers import Worker

# lg.basicConfig(level=lg.INFO)

generator_logger = lg.getLogger('data_providers.h5_file_insert.generators')


class BaseGenerator(Worker):
    def __init__(self,
                 out_queue,
                 batch_size,
                 shared_memory_pointer,
                 file_index_list,
                 translate_col_to_file_name=False,
                 worker_id=999,
                 max_batches=None,
                 delivery_function=None,
                 watched=False,
                 **kwargs):
        super(BaseGenerator, self).__init__(worker_id + GENERATOR_OFFSET, **kwargs)

        self.out_queue = out_queue
        self.batch_size = batch_size
        self.file_index_list = file_index_list
        self.shared_memory_pointer = shared_memory_pointer
        self.delivery_function = delivery_function
        self.max_batches = max_batches
        self.watched = watched
        self.translate_col_to_file_name = translate_col_to_file_name
        self.generator_id = self.worker_id - GENERATOR_OFFSET
        self.last_reader_index = None
        self.last_bucket_index = None

    def debug(self, message):
        if isinstance(message, list):
            message = " ".join(message)
        generator_logger.debug(" genera_id={0} ".format(self.worker_id) + message)
        # super(BaseGenerator, self).debug(" :GENERATOR #{0}: ".format(self.generator_id) + message)

    def info(self, message):
        if isinstance(message, list):
            message = " ".join(message)
        generator_logger.info(" genera_id={0} ".format(self.worker_id, self.batch_count) + message)
        # super(BaseGenerator, self).debug(" :GENERATOR #{0}: ".format(self.generator_id) + message)

    def generate(self):
        self.batch_count = 0

        # while not self.should_stop() or (self.max_batches is not None and count == self.max_batches):
        # while not self.should_stop() and (self.max_batches is None or self.batch_count < self.max_batches):
        while not self.should_stop() or (
                        self.max_batches is not None and self.batch_count >= self.max_batches):
            # Cleaning up
            if self.last_reader_index is not None and self.last_bucket_index is not None:
                self.debug("attempting to get lock to release buckets")
                if self.watched:
                    with self.shared_memory_pointer[self.last_reader_index][self.last_bucket_index][3].get_lock():
                        self.shared_memory_pointer[self.last_reader_index][self.last_bucket_index][3].value += 1
                else:
                    with self.shared_memory_pointer[self.last_reader_index][self.last_bucket_index][
                        0].get_lock():
                        self.shared_memory_pointer[self.last_reader_index][self.last_bucket_index][
                            0].value = 0

                self.debug(
                    "successfully got lock and released buckets last_reader_index={0} last_bucket_index={1}".format(
                        self.last_reader_index, self.last_bucket_index))

                self.last_reader_index = None
                self.last_bucket_index = None

            read_batch = None

            self.debug("attempting to get read_batch from out_queue")
            start_time = time.time()
            try:
                read_batch = self.out_queue.get(timeout=1)
            except Queue.Empty:
                # self.debug("out_queue empty, sleeping")
                self.sleep()
            finally:
                if read_batch is not None:
                    self.info(
                        "multi_or_out_queue_get_wait_time={0}".format(time.time() - start_time))
                    # self.info("multi_or_out_queue_get_wait_time={0} queue_size={1}".format(time.time() - start_time, self.out_queue.qsize()))
                    self.debug("successfully got a read_batch from the out_queue")
                    try:
                        reader_id, bucket_index, data_sets, batch_id = read_batch
                    except ValueError:
                        yield None

                    if self.watched:
                        with self.shared_memory_pointer[reader_id][bucket_index][2].get_lock():
                            self.shared_memory_pointer[reader_id][bucket_index][2].value += 1

                    payload = self.shared_memory_pointer[reader_id][bucket_index][1]

                    self.last_bucket_index = bucket_index
                    self.last_reader_index = reader_id

                    for batch_index in range(0, len(payload[0]), self.batch_size):
                        batch = range(len(payload))
                        for data_set_index, data_set in enumerate(payload):
                            batch[data_set_index] = data_set[
                                                    batch_index:batch_index + self.batch_size]

                        # generators get caught in this loop so redundant checks are necessary
                        # using that De Morgans law yo
                        if self.should_stop() or (
                                        self.max_batches is not None and self.batch_count == self.max_batches):
                            raise StopIteration

                        # batch = np.take(copy_of_payload, [range(batch_index, batch_index + self.batch_size)], axis=1)

                        self.debug("attempting to deliver a batch")
                        yield_wait_time = time.time()

                        if self.translate_col_to_file_name:
                            tmp_list = map(lambda x: [self.file_index_list[int(x[0])], int(x[1])],
                                           batch[self.translate_col_to_file_name])

                            # tmp_list = list()
                            # for index, item in enumerate(batch[self.translate_col_to_file_name]):
                            #     file_name = self.file_index_list[int(item[0])]
                            #     tmp_list.append([file_name, int(item[1])])

                            batch[self.translate_col_to_file_name] = tmp_list

                        if self.delivery_function is not None:
                            yield self.delivery_function(batch)
                        else:
                            yield tuple(batch)

                        self.debug(
                            "successfully delivered a batch, continuing from generator yield")
                        self.info("yield_wait_time={0}".format(time.time() - yield_wait_time))
                        self.batch_count += 1

        self.debug("exiting...")
        self.seppuku()
        raise StopIteration
