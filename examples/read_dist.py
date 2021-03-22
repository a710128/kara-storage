import os
import torch
import torch.distributed
import torch.utils.data
import kara_storage
from tqdm import tqdm

class MyDataset(torch.utils.data.IterableDataset):
    def __init__(self, ds):
        super().__init__()
        self.ds = ds

    def __iter__(self):
        for it in self.ds:
            yield {
                "src": it["src"],
                "index": it["index"],
            }
    
    def __len__(self):
        return len(self.ds)
    
def worker():
    print("Worker started")

def main():
    torch.distributed.init_process_group(backend='nccl', init_method='env://')

    rank = torch.distributed.get_rank()
    

    storage = kara_storage.RowStorage("file://kara_data")
    ds = storage.open("test", "a/b/c")
    

    ds = MyDataset(kara_storage.make_torch_dataset(ds, shuffle=True))
    dl = torch.utils.data.DataLoader(ds, batch_size=5)

    
    torch.distributed.barrier()

    cnt = 0
    for batch in tqdm(dl):
        cnt += 1
    torch.distributed.barrier()

if __name__ == "__main__":
    main()
