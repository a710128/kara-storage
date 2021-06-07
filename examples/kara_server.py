from kara_storage.rpc import KaraStorageServer

def main():
    server = KaraStorageServer("file://kara_data")
    server.bind("[::]:3030")
    server.start()
    server.join()

if __name__ == "__main__":
    main()