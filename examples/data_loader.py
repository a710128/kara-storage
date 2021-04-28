import kara_storage
import torch.utils.data as data

def main():
    storage = kara_storage.KaraStorage ( "file://kara_data" )
    
    # 初始化训练数据集
    ds_train = kara_storage.make_torch_dataset(
        storage.open("test", "my_dataset", "r", version="train"), 
        shuffle=True    # 随机打乱数据集
    )
    train_data_loader = data.DataLoader(
        ds_train, num_workers=2, 
        batch_size=8, 
    )

    # 初始化测试数据集
    ds_valid = kara_storage.make_torch_dataset(
        storage.open("test", "my_dataset", "r", version="valid"),
        begin=1, end=5, # 只使用测试集的 [1, 5) 部分
    )
    valid_data_loader = data.DataLoader(
        ds_valid, num_workers=2, 
        batch_size=8, 
    )


    for epoch in range(5):
        ds_train.set_epoch(epoch)
        ds_valid.set_epoch(epoch)

        print("Start epoch %d" % epoch)
        print("Train:")
        for it, batch in enumerate(train_data_loader):
            if it == 0:
                print(batch)
        print("Validate")
        for it, batch in enumerate(valid_data_loader):
            if it == 0:
                print(batch)


if __name__ == "__main__":
    main()
