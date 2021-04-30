import io
import kara_storage
import unittest, random
import torch.utils.data as data
import shutil, os

TEST_CASE_SIZE = 117
TEST_VERSIONS = [3, 1, 2, 4, 7, 6, False]
storage = kara_storage.KaraStorage("file://kara_data")

class TestLocalFileStorage(unittest.TestCase):
    def test_1_write(self):
        if os.path.exists("kara_data"):
            shutil.rmtree("kara_data")
        ds = storage.open_dataset("test", "a/b/c", "w", version="any-version")

        for i in range(TEST_CASE_SIZE):
            ds.write({
                "index": i,
                "bbb": "aaa",
                "ccc": i * 3.1415926 + 3
            })
        ds.close()


    def test_2_read_length_eof(self):
        ds = storage.open_dataset("test", "a/b/c", "r")
        self.assertEqual(len(ds), TEST_CASE_SIZE)
        for i, it in enumerate(ds):
            self.assertEqual(it["index"], i)
            self.assertEqual(it["bbb"], "aaa")
        
        with self.assertRaises(EOFError):
            ds.read()
        
    def test_3_read_random(self):
        ds = storage.open_dataset("test", "a/b/c", "r")
        
        idx = [i for i in range(TEST_CASE_SIZE)]
        random.shuffle(idx)

        for id_ in idx:
            v = ds[id_]
            self.assertEqual(v["index"], id_)
            self.assertEqual(v["bbb"], "aaa")
        
        v = ds.read()
        self.assertEqual(v["index"], 0)
        self.assertEqual(ds.tell(), 1)
    
    def test_4_read_pytorch_shuffle(self):
        ds = storage.open_dataset("test", "a/b/c", "r")

        pytorch_ds = kara_storage.make_torch_dataset(ds, shuffle=True)
        pytorch_ds.set_epoch(0)

        idx = []
        for it in data.DataLoader(pytorch_ds, num_workers=4):
            idx.append(it["index"].item())
            self.assertListEqual(it["bbb"], ["aaa"])
        
        sorted_idx = sorted(idx)
        for i, id_ in enumerate(sorted_idx):
            self.assertEqual(i, id_)
        
        with self.assertRaises(EOFError):
            ds.read()
        self.assertEqual(ds.tell(), ds.size())
        
    def test_5_read_pytorch_shuffle_repeatable(self):
        ds = storage.open_dataset("test", "a/b/c", "r")

        pytorch_ds = kara_storage.make_torch_dataset(ds, shuffle=True)
        pytorch_ds.set_epoch(0)

        idx = []
        for it in data.DataLoader(pytorch_ds, num_workers=1):
            idx.append(it["index"].item())
            self.assertListEqual(it["bbb"], ["aaa"])
        
        pytorch_ds.set_epoch(0)
        idx2 = []
        for it in data.DataLoader(pytorch_ds, num_workers=1):
            idx2.append(it["index"].item())
            self.assertListEqual(it["bbb"], ["aaa"])
        
        pytorch_ds.set_epoch(1)
        idx3 = []
        for it in data.DataLoader(pytorch_ds, num_workers=1):
            idx3.append(it["index"].item())
            self.assertListEqual(it["bbb"], ["aaa"])
        self.assertListEqual(idx, idx2)
        
        self.assertEqual(len(idx2), len(idx3))
        self.assertEqual(len(idx2), TEST_CASE_SIZE)

        all_same = True
        for a, b in zip(idx2, idx3):
            if a != b:
                all_same = False
                break
        self.assertFalse(all_same)
    
    def test_6_read_seek(self):
        ds = storage.open_dataset("test", "a/b/c", "r")

        length = len(ds)

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
                v = ds.read()
                self.assertEqual(v["index"], curr)
                curr += 1
    
    def test_7_read_slice(self):
        v = storage.open_dataset("test", "a/b/c", "r")

        ds = v.slice(5)
        it = iter(ds)
        self.assertEqual(next(it)["index"], 5)
        self.assertEqual(next(it)["index"], 6)
        self.assertEqual(next(it)["index"], 7)
        self.assertEqual(len(ds), len(v) - 5)

        ds = ds.slice(6, 2)
        it = iter(ds)
        self.assertEqual(next(it)["index"], 11)
        self.assertEqual(next(it)["index"], 12)
        with self.assertRaises(StopIteration):
            next(it)
        self.assertEqual(len(ds), 2)

    def test_8_write_append(self):
        ds = storage.open_dataset("test", "a/b/c", "w", version="latest")

        self.assertEqual(len(ds), TEST_CASE_SIZE)
        ds.write({
            "prp": "qrq",
        })
        ds.close()

        ds = storage.open_dataset("test", "a/b/c")
        ds.seek(1, io.SEEK_END)
        self.assertEqual(ds.read()["prp"], "qrq")
        self.assertEqual( len(ds), TEST_CASE_SIZE + 1 )

    def test_9_multi_version(self):
        for ver in TEST_VERSIONS:
            ds = storage.open_dataset("test", "test_multi_version", "w", version=ver)
            ds.write(ver)
            ds.close()
        
        for ver in TEST_VERSIONS:
            ds = storage.open_dataset("test", "test_multi_version", "r", version=ver)
            self.assertEqual( ds.read(), ver )
            self.assertEqual( len(ds), 1 )
        
        ds = storage.open_dataset("test", "test_multi_version", "r")
        self.assertEqual(ds.read(), TEST_VERSIONS[-1])


        

        