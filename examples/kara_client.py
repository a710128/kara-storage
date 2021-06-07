from kara_storage.rpc import KaraStorageClient
import time

def write():
    client = KaraStorageClient("localhost:3030")
    ds = client.open_dataset("rpc", "test", "w", version=time.time())
    
    for i in range(100):
        ds.write(str(i).encode("utf-8"))
    ds.close()

def read():
    client = KaraStorageClient("localhost:3030")
    ds = client.open_dataset("rpc", "test")
    for i in range(100):
        print(ds.read().decode("utf-8"))
    print(client.get_row_meta("rpc", "test"))

def main():
    write()
    read()

if __name__ == "__main__":
    main()