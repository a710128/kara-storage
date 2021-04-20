# 快速入门教程 —— 行存储

## 1. 在本地创建kara storage

```python
import kara_storage
storage = kara_storage.KaraStorage("file:///path/to/your/database")
```
其中 `/path/to/your/database` 是一个本地的路径，用于存放数据。

## 2. 创建一个数据集（行存储）

```python
dataset = storage.open("namespace", "dataset_name", "w", version=0)
```

在kara storage中，数据集根据`namespace`来进行隔离，通过`dataset_name`来指定数据集名称。

参数中的`"w"`表示要追加写入一个数据集，当数据集不存在时会自动创建。

## 3. 写入数据

```python
dataset.write({
    "msg": "hello"
})
dataset.write({
    "sentence": "world!"
})
```

在kara storage中，数据的单位是“条”（“行”），每次可以写入一条（行）数据。默认的数据序列化方法是json，当需要写入其它类型的数据时（例如numpy等），可以自己来实现serializer。

## 4. 关闭数据集

```python
dataset.close()
```

在数据写入完成后，要及时的关闭数据集（虽然很多时候不关闭也不会有问题）。

## 5. 读取数据

```python
dataset = storage.open("namespace", "dataset_name", "r", version="latest")

item1 = dataset.read() # { "msg": "hello" }
item2 = dataset.read() # { "sentence": "world!" }
```

以读取的模式（`"r"`）打开`dataset`，可以通过`version`来指定数据的版本号。当`verson = "latest"`时，会打开最近一次修改过的数据集。

数据可以通过 `dataset.read()` 的方式来读取，每次将会返回一条数据，数据的格式和写入的时候时一致的。

## 6. 移动读取指针

```python
dataset.seek(0, 0) # 移动到开头
dataset.seek(5, 1) # 跳过五条数据
dataset.seek(2, 2) # 移动到倒数第二条数据
```

`seek`函数包含两个参数`(offset, whence)`，其中`whence`有三种取值，分别表示：
* 0: 相对开头偏移 `offset`
* 1: 相对当前位置偏移 `offset`
* 2: 相对末尾偏移 `offset`

## 7. 其它的数据获取方法

kara storage的数据集还支持其它的数据访问方法

#### 7.1 通过循环遍历数据集

```python
dataset.seek(0, 0) # 在遍历之前，先确保将读取指针移动到数据集开头
for item in dataset:
    print(item)

## Output:
# { "msg": "hello" }
# { "sentence": "world!" }
```

除了使用`read`接口进行顺序的数据读取，kara行存储还支持使用`for`进行数据遍历。

#### 7.2 随机访问

```python
dataset[len(dataset) - 1] # { "sentence": "world!" }
```

除了顺序的数据读取，kara行存储还支持对于任意下标的访问。不过这样的访问方式是低效的，不建议在数据量较大时使用这种方法。

## 8. 对接Pytorch Dataset

```python
pytorch_ds = kara_storage.make_torch_dataset(dataset, shuffle=True)
```

kara storage提供了方便的`kara_storage.make_torch_datase`函数，来对kara行存储对象进行包装，并返回torch.utils.data.IterableDataset。

除此之外，我们还提供了自动的局部shuffle功能，在配合使用`DataLoader`时，请将`Dataloader`的`shuffle`参数设置为`False`。

在单机多卡的环境下，`make_torch_dataset`会自动根据`rank`来平均划分数据集，因此在结合`Dataloader`使用时，请不要使用`sampler`参数。

#### 训练时的要求

请在每一轮训练前使用`pytorch_ds.set_epoch(epoch_num)`来重置数据集读取指针，如果你启用了`shuffle`选项，这个操作也将重新打乱数据集。

__注意__： 对于 validation和test 数据集也应该在每次测试前调用 set_epoch 函数！
