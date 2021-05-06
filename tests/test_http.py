import kara_storage
import unittest, random
import os
import torch.utils.data as data

TEST_CASE_SIZE = 117

storage = kara_storage.KaraStorage(
    "https://kara-public.oss-cn-beijing.aliyuncs.com/test"
)

class TestHTTPSFileStorage(unittest.TestCase):
    def test_2_read_length_eof(self):
        ds = storage.open_dataset("test_ns", "mydb", mode="r")
        self.assertEqual(len(ds), TEST_CASE_SIZE)
        for i, it in enumerate(ds):
            self.assertEqual(it["index"], i)
            self.assertEqual(it["bbb"], "aaa")
        
        with self.assertRaises(EOFError):
            ds.read()
        
    def test_3_read_random(self):
        ds = storage.open_dataset("test_ns", "mydb", mode="r")
        
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
        ds = storage.open_dataset("test_ns", "mydb", mode="r")

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
        ds = storage.open_dataset("test_ns", "mydb", mode="r")

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
        ds = storage.open_dataset("test_ns", "mydb", mode="r")

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
        v = storage.open_dataset("test_ns", "mydb", mode="r")

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

    def test_8_load_directory(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            self.assertEqual(storage.load_directory("test", "a/b/c", tmpdir, progress_bar=False), "1")
            self.assertEqual(storage.load_directory("test", "a/b/c", tmpdir, version=0, progress_bar=False), "0")
            self.assertTrue( os.path.exists( os.path.join(tmpdir, "d1", "d.txt") ) )
            self.assertTrue( os.path.exists( os.path.join(tmpdir, "e1", "f.txt") ) )
            self.assertTrue( os.path.exists( os.path.join(tmpdir, "a.txt") ) )
            self.assertTrue( os.path.exists( os.path.join(tmpdir, "b.txt") ) )
            self.assertTrue( os.path.exists( os.path.join(tmpdir, "c.txt") ) )
            self.assertEqual( open(os.path.join( tmpdir, "d1", "d.txt" )).read(), "d1/d" )
            self.assertEqual( open(os.path.join( tmpdir, "e1", "f.txt" )).read(), "e1/f" )
            self.assertEqual( open(os.path.join( tmpdir, "a.txt" )).read(), "asdasd" )
            self.assertEqual( open(os.path.join( tmpdir, "b.txt" )).read(), "asdasd" )
            self.assertEqual( open(os.path.join( tmpdir, "c.txt" )).read(), "aaaabbb" )

            self.assertEqual(storage.load_directory("test", "a/b/c", tmpdir, version="1", progress_bar=False), "1")
            self.assertTrue( os.path.exists( os.path.join(tmpdir, "d1", "d.txt") ) )
            self.assertFalse( os.path.exists( os.path.join(tmpdir, "e1", "f.txt") ) )
            self.assertTrue( os.path.exists( os.path.join(tmpdir, "a.txt") ) )
            self.assertTrue( os.path.exists( os.path.join(tmpdir, "b.txt") ) )
            self.assertTrue( os.path.exists( os.path.join(tmpdir, "c.txt") ) )
            self.assertEqual( open(os.path.join( tmpdir, "d1", "d.txt" )).read(), "d1/d" )
            self.assertEqual( open(os.path.join( tmpdir, "a.txt" )).read(), "asdasd" )
            self.assertEqual( open(os.path.join( tmpdir, "b.txt" )).read(), "aaaabbb" )
            self.assertEqual( open(os.path.join( tmpdir, "c.txt" )).read(), "aaaabbb" )
