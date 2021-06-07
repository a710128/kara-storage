import io
import json
import random
import unittest
import multiprocessing as mp
from kara_storage.rpc import KaraStorageServer, KaraStorageClient

TEST_CASE_SIZE = 117
ADDRESS = "127.0.0.1:3080"

def start_rpc_server(evt):
    server = KaraStorageServer("file://kara_data")
    server.bind(ADDRESS)
    server.start()
    evt.set()
    server.join()

class TestRPCStorage(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        evt = mp.Event()
        cls.p_server = mp.Process(target=start_rpc_server, args=(evt,))
        cls.p_server.start()
        evt.wait()
    
    @classmethod
    def tearDownClass(cls) -> None:
        cls.p_server.terminate()
        if cls.p_server.join(timeout=10):
            cls.p_server.kill()
    
    def test_01_write(self):
        client = KaraStorageClient(ADDRESS)
        ds = client.open_dataset("rpc_test", "test", "w", version="v1")
        for i in range(TEST_CASE_SIZE):
            ds.write(json.dumps({
                "index": i,
                "bbb": "aaa",
                "ccc": i * 3.1415926 + 3
            }).encode("utf-8"))
        ds.close()
    
    def test_02_read_length_eof(self):
        client = KaraStorageClient(ADDRESS)
        ds = client.open_dataset("rpc_test", "test")

        self.assertEqual(ds.size(), TEST_CASE_SIZE)
        for i in range(TEST_CASE_SIZE) :
            it = json.loads(ds.read().decode("utf-8"))
            self.assertEqual(it["index"], i)
            self.assertEqual(it["bbb"], "aaa")
        
        with self.assertRaises(EOFError):
            ds.read()
    
    def test_03_read_random(self):
        client = KaraStorageClient(ADDRESS)
        ds = client.open_dataset("rpc_test", "test")
        
        idx = [i for i in range(TEST_CASE_SIZE)]
        random.shuffle(idx)

        for id_ in idx:
            v = json.loads(ds.pread(id_).decode("utf-8"))
            self.assertEqual(v["index"], id_)
            self.assertEqual(v["bbb"], "aaa")
        
        v = json.loads(ds.read().decode("utf-8"))
        self.assertEqual(v["index"], 0)
        self.assertEqual(ds.tell(), 1)
    
    def test_06_read_seek(self):
        client = KaraStorageClient(ADDRESS)
        ds = client.open_dataset("rpc_test", "test")

        length = ds.size()

        curr = 0
        ds.seek(curr)

        for _ in range(TEST_CASE_SIZE):
            id = random.randint(0, length - 1)
            whence = random.randint(0, 2)

            if whence == 0:
                curr = id
            elif whence == 1:
                curr += id
            elif whence == 2:
                curr = length - id
            if curr > length:
                curr = length
            if curr < 0:
                curr = 0

            ds.seek(id, whence)

            if curr >= length:
                with self.assertRaises(EOFError):
                    ds.read()
            else:
                v = json.loads(ds.read().decode("utf-8"))
                self.assertEqual(v["index"], curr)
                curr += 1
    
    def test_08_write_append(self):
        client = KaraStorageClient(ADDRESS)
        ds = client.open_dataset("rpc_test", "test", "w")

        self.assertEqual(ds.size(), TEST_CASE_SIZE)
        ds.write(json.dumps({
            "prp": "qrq",
        }).encode("utf-8"))
        ds.close()

        ds = client.open_dataset("rpc_test", "test")
        ds.seek(1, io.SEEK_END)
        self.assertEqual(json.loads(ds.read().decode("utf-8"))["prp"], "qrq")
        self.assertEqual( ds.size(), TEST_CASE_SIZE + 1 )

