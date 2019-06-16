### 设计一个高效的索引结构

#### 要求
- 某个机器的配置为：CPU 8 cores, MEM 4G, HDD 4T
- 1T无序数据文件，格式为 (key_size: uint64, key: bytes, value_size: uint64, value: bytes)，其中 1B <= key_size <= 1KB，1B <= value_size <= 1MB
- 设计一个索引结构，使得并发随机地读取每一个 key-value 的代价最小
- 允许对数据文件做任意预处理，但是预处理的时间计入到整个读取过程的代价里

#### 设计思路
- 如果key长度较小而value长度较大，则获取value无法避免一次磁盘随机读，那么只需要保证访问key的效率即可, 可用HashMap、B树等。
- 如果key长度较大而value长度较小，则获取key无法避免一次磁盘随机读，那么降低key的访问代价会带来性能提升。

但是题中key和value的长度范围区间较大，单一方案可能都无法适配，因此选取一个折中方案。

折中方案选取一个长度作为大value和小value的分割点。大value独立存储，小value和key一起存储。

因此索引结构分为两块数据，对应index和value两个文件。

index文件中存储key和较小的value，单文件key按照byte[]全局有序。

value文件中存储较大的value，是简单的二进制文件。

涉及的数据结构包括：
- SkipList: 用于内存排序，通过`skiplist.size.limit.mb`控制大小，skipList越大，查询的代价越小；
- Segment: 有序文件，包含多个`DataBlock`和一个`SegmentMetaBlock`；
- DataBlock: 较小的有序块，通过`<start_key, end_key>`来标识key的范围，同时通过更细粒度的`IndexBlock`来提高随机读性能；
- SegmentMetaBlock: 索引块，常驻内存；
- DataBlockCache: LRU缓存池，缓存`DataBlock`，大小由`block.cache.size.limit.mb`控制；
- KVRecord: KV的逻辑数据结构，通过一个标识位表示value类型(小value/大value)；
- ValueFile: 大value的存储文件，通过`value.file.size.limit.mb`控制单文件大小；
- IndexGenerator: 索引数据生成类，产生多一个`Segment`和`ValueFile`；
- KVManager: 查询的封装类；
- FlushHandler/FlushThread: 异步写入数据至磁盘，异步的并发度由`flush.thread.num`控制；
- Benchmark*: 测试类，简单模拟多线程并发读取，由于阻塞队列锁抢占较为耗时，所以在运行时通过`-b n`来控制一次查询访问n条记录；
- DataGenerator: 生成无序的KV数据；
- Main: 生成数据索引/测试查询；

#### 编译 + 测试
- mvn clean package
- 生成无序KV数据：data_generator.sh -d 目录 -s 总数据大小(MB) -l 单文件大小(MB); 数据格式: key_len(8字节) key value_len(8字节) value
- 生成并发测试数据：同上，或者直接挪取原始数据
- 测试:
  - 生成索引 + 测试: test_kv.sh -o 原始KV数据目录 -d 索引目录 -t 测试数据目录 [-t 并发线程数] [-b 测试时单批数据条数]
  - 使用已有索引 + 测试: test_kv.sh -d 索引目录 -t 测试数据目录 [-t 并发线程数] [-b 测试时单批数据条数]

#### 声明
- 该设计假设key具有全局唯一性，因此找到一个key就会返回。如果需要考虑重复key，需要合并/挑选多个segment的key；
- 测试时验证value，由于数据生成时无法保证key全局唯一，因此数据输出的`KV not match count`可能大于0；