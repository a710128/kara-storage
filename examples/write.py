import kara_storage

def main():
    storage = kara_storage.KaraStorage("file://kara_data")

    # 创建训练集
    ds_train = storage.open("test", "my_dataset", "w", version="train")
    for i in range(123):
        ds_train.write({
            "index": i,
            "bbb": "aaa",
            "ccc": i * 3.1415926 + 3
        })
    
    # 创建测试集
    ds_valid = storage.open("test", "my_dataset", "w", version="valid")
    for i in range(23):
        ds_valid.write({
            "index": i * 5,
            "bbb": "aaa",
            "ccc": i * 3.1415926 - 11
        })

if __name__ == "__main__":
    main()
