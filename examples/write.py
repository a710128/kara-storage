import kara_storage
import json
from tqdm import tqdm

def main():
    storage = kara_storage.KaraStorage("file://kara_data")
    ds = storage.open("test", "a/b/c", "w", version=1)

    fp = open("/data/private/zengguoyang/data.json", "r", encoding="utf-8")
    for i in tqdm(range(339397), total=339397):
        line = fp.readline()
        data = json.loads(line)
        data["index"] = i
        ds.write(data)

if __name__ == "__main__":
    main()
