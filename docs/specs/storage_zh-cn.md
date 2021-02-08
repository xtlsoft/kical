# 存储标准

（本文档仅供开发阶段开发人员交流使用）

## 引擎

使用 pebble 作为默认引擎。

由于初期设计的特殊性，将会将 kv 的内容做分区化同步，可能会单独出其它的 driver。

所有数据最终反应到多个 kv bucket。通常一个表对应一个 bucket。

## 数据类型

### Document 文档存储

每个 Document 可以分为两种类型：

1. 一维（包括部分字段使用 Gob 等存储）
2. 多维度

针对一维类型，将提供更多功能。

主键需要单独定义，分为 `auto-increment id`, `uuid.V4`
 和 `custom` 三种类型。

## K-V 对应

由于面向小数据集，将采用非常简单粗暴的 K-V 对应方案。

### Metadata

以 `chr(38) // &` 开头，后续不存在分隔符。

第二个字符为 `chr(58) // :` 表示这里定义了该表的存储类型。值为 `a` 代表标准行式文档存储（单个 Key 中保存一整行），为 `b` 表示标准列式文档存储（适用`列`列表选项，同时主键必须为 Auto-Increment ID），为 `c` 表示分析型文档存储（不适用图查询，单个键保存 `2^k` 个键，k 单独定义）

第二个字符为 `!` 第三个字符为 `k`：表示 k 的值。

第二个字符为 `|` 值中以 `|` 隔开存储键的名称列表和类型列表（类型在前，名称在后，类型占用一个 Byte）。

类型对应列表：

| chr |   typ   |
| --- | ------  |
|  0  | string  |
|  1  | integer |
|  2  | float   |
|  3  | decimal |
|  4  | time    |
|  5  | object  |
|  6  | tag     |
|  7  | enum    |

第二个字符为 `@` 值则为表名称。

第二个字符为 `*` 则定义主键类型和主键名称。值的第一个字符 (`0`: auto-increment id; `1`: uuid.V4; `2`: custom) 定义类型，之后的字符定义主键名称。

### 数据