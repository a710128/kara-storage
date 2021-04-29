import kara_storage
import random

def random_read(ds):
    print("Random access")
    length = len(ds)

    random.seed(0)
    idx = []
    for i in range(100):
        id = random.randint(0, length - 1)
        v = ds[id]
        idx.append(v["index"])
        assert id == idx[-1]
    print(idx[:10])
    
def sequential_read(ds):
    print("Sequential read")
    ds = kara_storage.make_torch_dataset(ds)
    ds.set_epoch(0)

    idx = []
    for v in ds:
        idx.append(v["index"])
    print(idx[:10])

def shuffle_read(ds):
    print("Shuffle read")
    ds = kara_storage.make_torch_dataset(ds, shuffle=True)
    ds.set_epoch(0)

    idx = []
    for v in ds:
        if len(idx) <= 10:
            idx.append(v["index"])
    print(idx[:10])

def seek_test(ds):
    print("Seek test")
    length = len(ds)

    random.seed(1)

    curr = 0
    ds.seek(curr)

    order = []
    
    for i in range(100):
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
        v = ds.read()

        if curr >= length:
            assert v is None
            order.append((id, whence, curr, None))
        else:
            assert v["index"] == curr
            order.append((id, whence, curr, v["index"]))
            curr += 1
    print(order[:10])

def slice_test(v):
    print("slice_test")
    ds = kara_storage.make_torch_dataset(v, begin=5)
    it = iter(ds)
    assert next(it)["index"] == 5
    assert next(it)["index"] == 6
    assert next(it)["index"] == 7
    assert len(ds) == len(v) - 5
    ds = kara_storage.make_torch_dataset(v, begin=7,end=9)
    it = iter(ds)
    assert next(it)["index"] == 7
    assert next(it)["index"] == 8
    try:
        v = next(it)
    except Exception as e:
        assert isinstance(e, StopIteration)
    else:
        assert AssertionError("get unexpected value : %s" % v)
    assert len(ds) == 2

def main():
    storage = kara_storage.KaraStorage ( "https://kara-public.oss-cn-beijing.aliyuncs.com/test/" )
    ds = storage.open("test_ns", "mydb", "r")
    random_read(ds)
    sequential_read(ds)
    shuffle_read(ds)
    seek_test(ds)
    slice_test(ds)

if __name__ == "__main__":
    main()
