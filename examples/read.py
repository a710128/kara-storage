import kara_storage
import random
from tqdm import tqdm

def random_read(ds):
    print("Random access")
    length = len(ds)

    random.seed(0)
    idx = []
    for i in tqdm(range(1000)):
        id = random.randint(0, length - 1)
        v = ds[id]
        idx.append(v["index"])
        assert id % 339397 == idx[-1]
    print(idx[:10])
    
def sequential_read(ds):
    print("Sequential read")
    ds = kara_storage.make_torch_dataset(ds)

    idx = []
    for v in tqdm(ds):
        if len(idx) <= 10:
            idx.append(v["index"])
    print(idx[:10])

def shuffle_read(ds):
    print("Shuffle read")
    ds = kara_storage.make_torch_dataset(ds, shuffle=True)
    ds.set_epoch(0)

    idx = []
    for v in tqdm(ds):
        if len(idx) <= 10:
            idx.append(v["index"])
    print(idx[:10])

def seek_test(ds):
    ds.seek(1, 2) # go to the last one
    print(ds.tell())
    v = ds.read()
    assert v["index"] % 339397 == 339396
    ds.seek(128, 0) # go to the 129th item
    print(ds.tell())
    assert ds.read()["index"] == 128
    ds.seek(5, 1)   # go to the 135th item
    print(ds.tell())
    assert ds.read()["index"] == 134


def main():
    storage = kara_storage.KaraStorage (
        "oss://oss-cn-beijing.aliyuncs.com/rich-spider", 
        app_key="***APP_KEY***", 
        app_secret="***APP_SECRET***"
    )
    ds = storage.open("test", "a/b/c", "r")

    random_read(ds)
    sequential_read(ds)
    shuffle_read(ds)
    seek_test(ds)

if __name__ == "__main__":
    main()
