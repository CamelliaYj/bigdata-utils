# bigdata-utils

## JSqlParserDdlParser：根据最新DDL生成旧表缺失字段简易工具

### 1. 背景

为了解决大表的schema更新问题，写了一个简易工具，通过对比新旧表的DDL，自动补全旧表中缺失的字段，以减少手动操作的工作量。

### 2. 输入

1. 多张历史表建表DDL。
2. 目标表建表DDL。

说明：这里并不对建表DDL做语法判断，错误的DDL会得到错误的结果，默认所提供的DDL全部正确。

### 3. 输出

结果输出三列：miss_col_name,miss_col_type,miss_col_ddl。例如：

| miss_col_name | miss_col_type | miss_col_ddl | 
| :-----| :-----| :-----| 
| col1 | int | null as col1 |
| col2 | string | '' as col2 |






