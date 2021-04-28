import kara_storage

def main():
    storage = kara_storage.KaraStorage("file://kara_data")
    ds = storage.open("test", "my_dataset", "r", version=0)

    for i in range(10):
        print(ds.read())
    ds.close()

if __name__ == "__main__":
    main()
