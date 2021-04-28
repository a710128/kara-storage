import kara_storage
import torch.utils.data as data
def main():
    storage = kara_storage.KaraStorage ( "file://kara_data" )
    ds = storage.open("test", "a/b/c", "r")    
    ds = kara_storage.make_torch_dataset(ds)
    data_loader = data.DataLoader(ds, num_workers=4)
    for it in data_loader:
        print(it)

if __name__ == "__main__":
    main()

