# KARA Storage

KARA 平台存储模块！！！

## 1. 介绍

kara_storage是KARA平台存储模块的Python SDK，目前提供了行存储的服务，在将来还会提供对象存储的服务。

### 1.1 行存储
__行存储__ 以条为单位实现数据的写入和读取，可以轻松的完成大规模训练数据的存储，理论上可以支持任意形式的数据（包括类和bytes，不过目前没有提供相关接口）。

SDK提供流式读取服务和流式Shuffle服务。在本地硬盘上，可以达到 `30000it/s`的追加写入，`1000it/s`的随机读取，`50000it/s`的顺序读取，`40000it/s`的shuffle读取性能（测试于服务器`166.111.121.22`）。

### 1.2 对象存储
__对象存储__ 是一种以文件为单位的 Key-Value 数据库，可以实现各种尺寸的文件的存储。在kara_storage中，对象存储用于保存目录，例如模型的训练结果等


### 1.3 安装方法

将源代码保存到本地后，输入一下命令即可安装。

```console
$ python setup.py install
```

## 2. 使用说明

### 2.1 打开数据库

##### 使用绝对路径打开本地数据库

```python
import kara_storage
storage = kara_storage.RowStorage("file:///path/to/your/database")
```
其中`/path/to/your/database`表示数据库的绝对路径。

##### 使用相对路径打开本地数据库

```python
import kara_storage
storage = kara_storage.RowStorage("file://my/databse")
```
其中 `my/database` 表示数据库在当前工作目录下的相对路径。

### 2.2 打开数据集

```python
dataset = storage.open("namespace", "dataset_name", "r", version="latest")
```

`open`命令会返回一个dataset实例，用于后继数据的写入和读取，它主要包含四个参数：

* namespace: “命名空间”。KARA Storage提供基于命名空间的数据隔离。
* key: “数据集名称”。在命名空间下，可以有多个数据集，数据集之间以`key`进行区分。
* mode: "r"表示读，"w"表示写。
* version: “数据集版本”。KARA Storage提供了同一数据集不同版本的管理，当不指定`version`或将`version`指定为`"latest"`时，会自动打开最新创建的版本。版本的名称可以是数字或者字符串。

如果要打开的数据集不存在，`open`命令会自动创建一个对应的数据集，在创建新数据集时，必须要指定数据集的`version`，否则会报错或打开最新一次修改的数据集。

### 2.3 读取数据集

```python
data = dataset.read()
```
使用 `read` 接口即可从数据集中读取一条数据，多次的调用`read`会自动的依次读取下去。

对于同一个数据集，从里面读取出的数据的顺序总是和写入时相同。当数据被读取完时，将会返回`None`。

### 2.4 追加写入数据集

```python
ok = dataset.write(obj)
```
使用`write`接口可以在数据集的末尾追加写入一条数据，它需要传入一个参数`obj`。这个`obj`在默认情况下将被序列化为json对象，你可以使用`storage.open`接口中的`serialization`来控制这一过程，例如使用我们提供的其它序列化方案（pickle）或者使用自己的序列化方法。

`write`接口将会把数据写入缓冲区，在刷新缓冲区之前，数据不会被持久化，你可以手动的调用`dataset.flush()`来强制刷新缓冲区，当然这样可能会带来额外的开销。

__提示__ : 在调用`dataset.close()`时，`flush`也会被自动的调用。

### 2.5 移动读取指针

```python
dataset.seek(offset, whence)
```
类似于文件操作，KARA Storage的行存储也支持读取指针的移动，指针移动的最小单位为 _“1条数据”_ 。

##### whence = 0
`whence = 0`表示移动到距离数据集开头`offset`的地方。

例如：`dataset.seek(0, 0)`表示移动到数据集的开头，在此时调用`read`接口会返回数据集的第一条数据。

##### whenc = 1
`whence = 1`表示根据当前指针位置偏移`offset`个数据。

例如：`dataset.seek(1, 1)`表示跳过下一跳数据。

##### whence = 2
`whence = 2`表示移动到距离数据集末尾`offset`的地方。

例如：`dataset.seek(0, 2)`表示移动到数据集末尾，此时调用`read`接口会返回`None`。

### 2.6 和Pytorch对接

以上接口提供了简单的流式访问，为了更好的支持pytorch的d`DataLoader`，我们提供了KARA行存储对`torch.utils.data.IterableDataset`的包装。

```python
import kara_storage
pytorch_ds = kara_storage.make_torch_dataset(dataset)

pytorch_ds.set_epoch(0) # 设置当前epoch
```

通过`make_torch_dataset`接口可以快速的将`dataset`包装为Pytorch所支持的`IterableDataset`类型。

##### distributed支持

在调用完`torch.distributed.init_process_group`后调用`make_torch_dataset`接口会自动的根据当前`rank`来分割数据集，实现对分布式训练的支持。

一份完整的代码可以参考 `examples/read_dist.py`

__注意__ ： 在多机训练环境下，数据库需要能被每台机器都访问到。

##### shuffle支持

```python
import kara_storage
kara_storage.make_torch_dataset(dataset, shuffle=True)
```

通过在调用`make_torch_dataset`时，传入参数`shuffle=True`可以实现伪随机的数据打乱，在此时可以传入额外的参数对打乱算法进行控制。

额外参数：

* seed：打乱算法的随机数种子（int类型）
* buffer_size: 打乱缓冲区的大小（应当是一个整数，越大时占用内存越大，但打乱随机性也越好）
* shuffle_ratio: 每次打乱的比例（0~1之间的浮点数）

在默认情况下，这些参数的值为`seed = 0`，`buffer_size = 10240`，`shuffle_ratio = 0.1`

## 3. 可复现性

在数据集固定、GPU数量固定、随机参数（seed, buffer_size, shuffle_ratio）固定时，`make_torch_dataset`接口返回的数据集总能以相同的顺序读取出相同的数据。

