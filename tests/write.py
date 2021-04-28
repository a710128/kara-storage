import kara_storage

def main():
    storage = kara_storage.KaraStorage("file://kara_data")
    ds = storage.open("test", "a/b/c", "w", version=1)

    for i in range(123):
        ds.write({
            "index": i,
            "bbb": "aaa",
            "ccc": i * 3.1415926 + 3
        })
    ds.close()

if __name__ == "__main__":
    main()
