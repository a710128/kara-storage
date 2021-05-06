import io
import kara_storage
import unittest, random
import torch.utils.data as data
import shutil, os
import tempfile

TEST_CASE_SIZE = 117
TEST_VERSIONS = [3, 1, 2, 4, 7, 6, False]
storage = kara_storage.KaraStorage("file://kara_data")

class TestLocalFileStorage(unittest.TestCase):
    def test_01_write(self):
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


    def test_02_read_length_eof(self):
        ds = storage.open_dataset("test", "a/b/c", "r")
        self.assertEqual(len(ds), TEST_CASE_SIZE)
        for i, it in enumerate(ds):
            self.assertEqual(it["index"], i)
            self.assertEqual(it["bbb"], "aaa")
        
        with self.assertRaises(EOFError):
            ds.read()
        
    def test_03_read_random(self):
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
    
    def test_04_read_pytorch_shuffle(self):
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
        
    def test_05_read_pytorch_shuffle_repeatable(self):
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
    
    def test_06_read_seek(self):
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
    
    def test_07_read_slice(self):
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

    def test_08_write_append(self):
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

    def test_09_multi_version(self):
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
    
    def test_10_save_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            t1 = os.path.join(tmpdir, "t1")
            t2 = os.path.join(tmpdir, "t2")
            t3 = os.path.join(tmpdir, "t3")
            
            # make version 0
            os.makedirs(t1)
            open( os.path.join(t1, "f1"), "w" ).write("v0/f1")
            open( os.path.join(t1, "f2"), "w" ).write("v0/f2")
            os.makedirs(os.path.join(t1, "d1"))
            open( os.path.join(t1, "d1", "f3"), "w" ).write("v0/f3")
            self.assertEqual(storage.save_directory("test", "a/b/c", t1, progress_bar=False), "0")
            
            # make version 1
            open( os.path.join(t1, "d1", "f3"), "w" ).write("v1/f3")
            open( os.path.join(t1, "f2"), "w" ).write("v1/f2")
            self.assertEqual(storage.save_directory("test", "a/b/c", t1, progress_bar=False), "1")
            
            # load version latest
            self.assertEqual(storage.load_directory("test", "a/b/c", t2, progress_bar=False), "1")
            self.assertEqual(open(os.path.join(t2, "f1"), "r").read(), "v0/f1")
            self.assertEqual(open(os.path.join(t2, "f2"), "r").read(), "v1/f2")
            self.assertEqual(open(os.path.join(t2, "d1", "f3"), "r").read(), "v1/f3")
            
            # load version 0
            self.assertEqual(storage.load_directory("test", "a/b/c", t3, version=0, progress_bar=False), "0")
            self.assertEqual(open(os.path.join(t3, "f1"), "r").read(), "v0/f1")
            self.assertEqual(open(os.path.join(t3, "f2"), "r").read(), "v0/f2")
            self.assertEqual(open(os.path.join(t3, "d1", "f3"), "r").read(), "v0/f3")




        

        