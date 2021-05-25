# KARA Storage

KARA 平台存储模块！！！

## 1. 介绍

kara_storage是KARA平台存储模块的Python SDK，目前提供了行存储和对象存储的服务。

### 1.1 行存储
__行存储__ 以条为单位实现数据的写入和读取，可以轻松的完成大规模训练数据的存储，理论上可以支持任意形式的数据（包括类和bytes，对于任意类型的数据支持请实现自己的Serializer）。

__性能__

SDK提供流式读取服务和流式Shuffle服务，我们在大小为1011MB的实际数据上进行了测试。

| 测试环境         | 追加写入(it/s)  | 乱序读取(it/s)  | 顺序读取(it/s)  | Shuffle读取(it/s)  |
| -------------- | -------------- | -------------- | -------------- | ----------------- |
| 166.111.121.22 | 29242.59       | 1127.09        | 51883.92       | 39414.84          |
| 166.111.5.239 NFS | 8761.2      |  138.91        | 33353.86       | 15552.44          |
| 103.242.175.117 NFS | 11897.37  | 652.74         | 34254.19       | 25636.74          |
| 103.242.175.227 NFS | 30536.63  | 2220.22        | 87475.19       | 64527.47          |



| 测试环境         | 追加写入(MB/s)  | 乱序读取(MB/s)  | 顺序读取(MB/s)  | Shuffle读取(MB/s)  |
| -------------- | -------------- | -------------- | -------------- | ----------------- |
| 166.111.121.22 | 88.241         | 3.401          | 156.56         |  118.94           |
| 166.111.5.239 NFS | 26.44       | 0.42           | 100.65         |  46.93            |
| 103.242.175.117 NFS | 35.90     | 1.97           | 103.36         |  77.36            |
| 103.242.175.227 NFS | 92.15     | 6.70           | 263.96         |  194.72           |


### 1.2 对象存储
__对象存储__ 是一种以文件为单位的 Key-Value 数据库，可以实现各种尺寸的文件的存储。在kara_storage中，对象存储用于保存目录，例如模型的训练结果等

目前工具包提供了 `load_directory` 和 `save_directory` 两个接口的支持。


### 1.3 安装方法

将源代码保存到本地后，输入一下命令即可安装。

#### 1.3.1 PIP（推荐）
```console
$ pip install kara-storage
```

#### 1.3.2 clone安装
将仓库克隆到本地，并在项目目录中执行以下命令即可完成安装。

```console
$ python setup.py install
```

## 2. 使用说明

### 2.1 打开数据库

##### 使用绝对路径打开本地数据库

```python
import kara_storage
storage = kara_storage.KaraStorage("file:///path/to/your/database")
```
其中`/path/to/your/database`表示数据库的绝对路径。

##### 使用相对路径打开本地数据库

```python
import kara_storage
storage = kara_storage.KaraStorage("file://my/databse")
```
其中 `my/database` 表示数据库在当前工作目录下的相对路径。

##### 打开阿里云上的数据库

```python
import kara_storage
storage = kara_storage.KaraStorage("oss://OSS_ENDPOINT/YOUR_BUCKET_NAME", app_key="*** APP KEY ***", app_secret="*** APP SECRET ***")
```

其中`OSS_ENDPOINT`表示oss的节点，例如`oss-cn-beijing.aliyuncs.com`。

在使用阿里云上的数据库前，请确保你的APP KEY和APP SECRET有权限访问数据库。

##### 打开HTTP数据库

```python
import kara_storage
storage = kara_storage.KaraStorage("https://path-to-your-dataset/prefix")
```

可以结合阿里云OSS实现简单的数据公开。


### 2.2 行存储
#### 2.2.1 打开数据集

```python
dataset = storage.open("namespace", "dataset_name", "r", version="latest")
```

`open`命令会返回一个dataset实例，用于后继数据的写入和读取，它主要包含四个参数：

* namespace: “命名空间”。KARA Storage提供基于命名空间的数据隔离。
* key: “数据集名称”。在命名空间下，可以有多个数据集，数据集之间以`key`进行区分。
* mode: "r"表示读，"w"表示写。
* version: “数据集版本”。KARA Storage提供了同一数据集不同版本的管理，当不指定`version`或将`version`指定为`"latest"`时，会自动打开最新创建的版本。版本的名称可以是数字或者字符串。

如果要打开的数据集不存在，`open`命令会自动创建一个对应的数据集，在创建新数据集时，必须要指定数据集的`version`，否则会报错或打开最新一次修改的数据集。

#### 2.2.2 读取数据集

```python
data = dataset.read()
```
使用 `read` 接口即可从数据集中读取一条数据，多次的调用`read`会自动的依次读取下去。

对于同一个数据集，从里面读取出的数据的顺序总是和写入时相同。当数据被读取完时，将会返回`None`。

#### 2.2.3 追加写入数据

```python
ok = dataset.write(obj)
```
使用`write`接口可以在数据集的末尾追加写入一条数据，它需要传入一个参数`obj`。这个`obj`在默认情况下将被序列化为json对象，你可以使用`storage.open`接口中的`serialization`来控制这一过程，例如使用我们提供的其它序列化方案（pickle）或者使用自己的序列化方法。

`write`接口将会把数据写入缓冲区，在刷新缓冲区之前，数据不会被持久化，你可以手动的调用`dataset.flush()`来强制刷新缓冲区，当然这样可能会带来额外的开销。

__提示__ : 在调用`dataset.close()`时，`flush`也会被自动的调用。

#### 2.2.4 移动读取指针

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

#### 2.2.5 实现自己的序列化方法

用户可以通过实现自己的`Serializer`来使用自定义的方法序列化数据，同时也可以使用我们内置的其它方法来替换我们的默认序列化方法。

##### 编写自己的 Serializer

```python
import kara_storage
import pickle

class MySerializer(kara_storage.serialization.Serializer):
    def serialize(self, x): # 序列化x，将x转换为bytes
        return pickle.dumps(x)
    
    def deserialize(self, x): # 反序列化x，将x从bytes重新转换回对象
        return pickle.loads(x)
```

##### 使用自己的序列化方法

```python
import kara_storage
dataset = storage.open("namespace", "dataset_name", "r", version="latest", serialization=MySerializer())
```

##### 其它内置序列化方法

* kara_storage.serialization.NoSerializer: 直接将bytes数据写入数据库
* kara_storage.serialization.PickleSerializer: 将对象使用pickle序列化后存入数据库
* kara_storage.serialization.JSONSerializer: 将数据转换为json字符串存入数据库


#### 2.2.6 和Pytorch对接

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

#### 2.2.7. 可复现性

在数据集固定、GPU数量固定、随机参数（seed, buffer_size, shuffle_ratio）固定时，`make_torch_dataset`接口返回的数据集总能以相同的顺序读取出相同的数据。

### 2.3 对象存储

#### 2.3.1 从服务器加载对象

```python
storage.load_directory("namespace", "object_name", "local_path", "version")
```

`load_d_irectory`会返回一个字符串，表示当前加载的对象的版本。它主要包含4个参数：

* namespace: 命名空间
* object_name: 要加载的对象名称
* local_path: 要加载到的本地路径
* version: 要加载的数据版本，默认为`"latest"`，即表示加载最新的版本

#### 2.3.2 将本地对象上传到服务器

```python
storage.save_directory("namespace", "object_name", "local_path", "version")
```

`save_directory`会返回一个字符串，表示当前保存的对象的版本。它主要包含4个参数：
* namespace: 命名空间
* object_name: 要加载的对象名称
* local_path: 要加载到的本地路径
* version: 要加载的数据版本，默认为`None`，表示自动生成一个不重复的版本号

## 3. KARA Storage CLI

为了方便大家的使用，我们还提供了命令行工具来进行简单的数据查看和上传下载。

### 3.1 查看数据

可以使用以下命令进行数据的可视化查看（目前仅支持json序列化）。
```console
$ kara_storage row <url> view <namespace> <key> [-v version] [--app-key app_key] [--app-secret app_secret] [--begin data_index]
```

__例如__

```console
$ kara_storage row https://kara-public.oss-cn-beijing.aliyuncs.com/test view test_ns mydb
```

### 3.2 上传文件

```console
$ kara_storage obj <url> save <namespace> <key> <local_path> [-v version] [--app-key app_key] [--app-secret app_secret]
```
该命令会将本地路径`local_path`上传，并设置版本号为`version`，默认会新建一个版本。

### 3.3 文件下载

```console
$ kara_storage obj <url> load <namespace> <key> <local_path> [-v version] [--app-key app_key] [--app-secret app_secret]
```
该命令会将文件加载到`local_path`目录。

## 4. 其它

欢迎大家测试、提issue！

# 更新日志

### 2.0.3

* 修复了 kara_storage 对 oss2 依赖的问题
* 修复了在 load_directory 和 save_directory 时，使用 Ctrl+C 无法终止程序的问题


### 2.0.2

* 修复了blessed依赖问题
* CLI添加打印版本号

### 2.0.1

* 添加了文件上传和下载的进度条
* 添加了CLI支持

### 2.0.0

* 添加了 HTTP/HTTPS 存储后端的支持
* 完善了对象存储系统，实现了基于hash的数据压缩和文件续传
* 添加了RowDataset的多线程支持
* 重构了大部分代码

### 1.0.3

* 添加对 pytorch DataLoader 中 num_workers > 0 时候的支持
* 添加对 OSS 前缀访问的支持
  *  现在可以通过 `oss://endpoint/bucket/prefix` 的方法来指定使用oss存储时的前缀
* 修复 `"r"` 模式下可以修改数据集的问题

### 1.0.2

* 添加对线程安全的支持
  * 在RowDataset中添加了线程锁 `threading.Lock`
* 添加pytorch dataset的slice支持
  * 通过 `kara_storage.make_torch_dataset( dataset, begin=a, end=b )` 来实现对数据集 `[a, b)` 区间的访问。
