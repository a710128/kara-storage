import kara_storage

def main():
    storage = kara_storage.KaraStorage("file://kara_data")
    
    # 读取训练集
    ds_train = storage.open("test", "my_dataset", "r", version="train")
    print("Size of training dataset: %d" % len(ds_train))
    for i in range(10):
        print(ds_train.read())
    
    # 读取测试集
    ds_valid = storage.open("test", "my_dataset", "r", version="valid")
    print("Size of validation dataset: %d" % len(ds_valid))
    for i in range(10):
        print(ds_valid.read())
    

if __name__ == "__main__":
    main()
