## 影像平台相关概念介绍

假如现一个影像平台中有以下表：  
```
- 主表 ABC.ABC_DOC（批次信息表）
	- 子表 ABC.ABC_DOC_2024
- 主表 ABC.ABC_FILE（文件信息表）
    - 子表 ABC.ABC_FILE_202401
- 普通表 ABC_LOB_20240101.ABC_LOB_20240101（LOB表）
```

可得出信息：  

1. 表类型分别为 DOC 表，FILE 表和 LOB 表，这是个代称，有些客户可能会叫其他类型，比如把 FILE 文件信息表叫 PART 表。  
	这个表类型非常关键，会在此工具中各个地方关联起同一类型的表。（输出文件，配置文件等）  

2. 业务应用名为 ABC。
3. 模型名分别为：
	```
	$APPNAME.$APPNAME.DOC, $APPNAME.$APPNAME.DOC_$YYYY
    $APPNAME.$APPNAME.FILE, $APPNAME.$APPNAME.FILE_$YYYY$MM
    $APPNAME_LOB_$YYYY$MM$dd.$APPNAME_LOB_$YYYY$MM$dd
	```

4. 某些客户的 LOB 表是普通表，到下一个时间时由他们业务进行切换；某些客户则为主子表，此时 LOB 的子表也有对应的模型名


## 使用背景
1. 目前影像平台的常见需求由：机器扩容、数据迁移和扩建下一时间分区的表。
2. 每个客户影像平台模型各有差异，常见模型有：  
   - SDB: DOC 表，FILE 表用于存储对象元数据，LOB 表用于存储对象。  
   - SCM: LOB 表由 SCM 管理，其余同 SDB。  
   即使表类型一致，但业务量不同，也会使得各客户间扩建表的时间间隔不同，如年季月日。  
3. 没有此工具前，前线同学执行上述影像平台任务时都需要人为处理对应平台的信息，且只能输出一份仅适用于该任务的扩建脚本，在同类型任务上浪费了人力资源。
4. 此工具可以帮助前线同学通过客户的快照信息，本地快速分析客户的部署模型，数据量分布和扩建表信息，以便前线同学完成任务。


## 工具简介
- `bin/check_model.js`
	- 分析生成影像平台的部署模型、现有表数据量分布和后续扩建表信息，并与标准模型对比得出当前目前影响平台模型是否为标准。
	- 运行模式(MODE)：
		- collect, 收集快照信息。
		- run, 基于收集的快照信息进行分析；如果无法在生产环境分析，可以把收集的快照文件传到自己的环境分析。
- `bin/create_domain.js`
    - 根据 domain.csv 配置在对应组上建域，具有幂等性。
    - 运行模式(MODE)：
        - test, 打印将会创建的域的信息。
        - run, 创建域。
        - rollback, 回滚创建的域，要求域为空，即没有 CS。
- `bin/add_cl.js`
    - 根据 check_model.js 生成的扩建表信息文件 output/add_cl.js 扩展子表和普通表，具有幂等性。
    - 根据客户新业务使用 conf/add_cl.js 新建主子表和普通表，具有幂等性。
    - 运行模式(MODE)：
        - test, 打印将会创建的表的信息。
        - run, 创建表。
        - rollback, 回滚创建的表，要求 CL 中记录数和 LOB 数都为空；若回滚时发现 CS 中没有 CL，会将 CS 一起回滚。
- 运行方法，在根目录下执行 `sdb -e 'var MODE = "\<MODE\>"' -f bin/\<TOOL\>.js`


##工具目录
```
|- bin      
	|- add_cl.js                创建 CL 的工具
	|- check_model.js           消息收集和模型分析工具
	|- create_domain.js         创建 DOMAIN 的工具
	|- args.js                  基本配置参数
	|- general.js               工具通用函数实现，使用时无需关心
|- conf
	|- add_cl.csv               新增业务模型配置模板
	|- config.json              CL 和 INDEX 通用配置模板
	|- domain.csv               DOMAIN 配置模板
	|- model               		现有影像平台标准模型目录，用于 `check_model.js` 工具分析模型后进行对比，对比结果没有实际影响
|- log                     		工具运行生成的日志文件目录，命名格式为 <工具名>_<模式>_<运行时间>.log
|- output   			   		工具输出的分析文件
	|- snapshot_cata.out   		check_model.js 工具在 collect 模式使用内置 SQL 收集的 $SNAPSHOT_CATA 快照信息
	|- snapshot_cl.out     		check_model.js 工具在 collect 模式使用内置 SQL 收集主节点的 $SNAPSHOT_CL 快照信息
	|- snapshot_cs.out     		check_model.js 工具在 collect 模式使用内置 SQL 收集主节点的 $SNAPSHOT_CS 快照信息
	|- snapshot_system.out      check_model.js 工具在 collect 模式使用内置 SQL 收集的 $SNAPSHOT_SYSTEM 快照信息
	|- list_cs.out              check_model.js 工具在 collect 模式使用内置 SQL 收集的 $LIST_CS 快照信息
	|- current_model.json       check_model.js 工具在 run 模式分析生成的影像平台模型信息
	|- information_by_cl.csv    check_model.js 工具在 run 模式分析生成的表级的单副本统计数据
	|- current_model.json       check_model.js 工具在 run 模式分析生成的表类型级的统计数据
	|- current_model.json       check_model.js 工具在 run 模式分析生成的影像平台扩建表信息
```


##工具配置文件
- `bin/args.js`
	- 供所有工具使用。
	- 基本的连接 SDB 所需参数，以及工具必要参数，某些非必要的参数在各自工具 js 的开头中修改。
- `conf/domain.csv`
	- 供 `bin/create_domain.js` 工具使用
	- 此文件为 json 格式。
	- AutoSplit 无法配置，默认为 true。
    - 此配置文件默认模板为新建两个域，分别为：  
		- 域名：domain_1  所在组 group1  
        - 域名：domain_2  所在组 group1,group2,group3  
- `add_cl.csv`
	- 供 `bin/add_cl.js` 工具使用，使用时需要注意使用的配置文件的路径是否正确。
    	- 配置文件模板为 `conf/add_cl.csv`，模板内容为新建新业务应用。
		- 运行 `bin/check_model.js` 工具后，会生成一份扩建当前集群表的配置文件 `output/add_cl.csv`
    - 字段介绍，除以下配置，其余创建 CL,INDEX 所涉及参数均在 `conf/config.json` 中进行通用配置。  

		|字段名|描述|
		|---|---|
		|appName|新建的业务应用名|
		|mainCLName|主表名，只有子表需要填写|
		|createCLName|新建表名|
		|type|表类型|
        |domain|新建表所在的域|
        |groupNum|新建表所占域的组数，若为空，则全域打散；若不为空，则按照 2 的 n 次方向上取整；如果需要全域打散，保持为空
        |shardingKey|子表挂载到主表的 shardingkey，主表和普通表保持为空|
        |lowBound|子表挂载到主表的 lowBound，如果需要 MIN 分区则填写 MIN，主表和普通表保持为空
        |upBound|子表挂载到主表的 upBound，如果需要 MAX 分区则填写 MAX，主表和普通表保持为空
        |detachCL|子表挂载时需要临时卸载的分区，由 `check_model.js` 生成时为 MAX 分区；如果是新建业务应用表，需要按顺序填写子表，把 MAX 表写在最后，无需填写 detachCL
        |pageSize|如果需要创建 CS，CS PageSize 的值
        |lobPageSize|如果需要创建 CS，CS LobPageSize 的值
        |partition|CL Partition 的值|
        |indexType|两种取值，由 `check_model.js` 生成时为 inherit，表示继承之前表的索引；如果是新建业务应用表，需要为 general，表示使用通用配置|
        |isMainCL|是否为主表，仅在创建新主子表时使用|
- `conf/config.json`
	- 此配置文件模板为新建新表和索引的通用配置，某些配置如 PageSize,lowBound,upBound 等此处无法配置，需要在 `add_cl.csv` 中针对每个 CL 单独配置。
	- 此配置文件中 index 级别的配置仅当 `add_cl.csv` 中 indexType 为 general 时才生效。
	- 配置介绍

		|级别|配置项|描述|
		|---|---|---|
		|cl|type|表类型|
		|cl|ShardingKey.MainCL|主表 ShardingKey 字段，用于子表挂载|
		|cl|ShardingKey.SubCL|子表 ShardingKey 字段|
		|cl|ShardingKey.NormalCL|普通表 ShardingKey 字段|
		|cl|ShardingKeyType.MainCL|主表 ShardingKeyType, 目前仅支持 range|
		|cl|ShardingKeyType.SubCL|子表 ShardingKeyType, 目前仅支持 hash|
		|cl|ShardingKeyType.NormalCL|普通表 ShardingKeyType 字段，目前仅支持 hash|
		|cl|other|其余配置项均是官网 createCL 的配置，均为默认值，详见官网|
		|index|type|表类型|
		|index|mainCLNeedCreate|是否需要在主表上创建索引，默认为 false|
		|index|index|索引配置，均是官网 createIndex 的配置，均为默认值，详见官网|

## 工具使用场景及步骤
### 机器扩容和数据迁移场景
1. 配置 args.js 中变量
2. 收集影像平台快照信息

	```
	sdb -e 'var MODE = "collect"' -f check_model.js
	```

3. 分析收集到的信息

	```
	sdb -e 'var MODE = "run"' -f check_model.js
	```

	如果不允许在生产环境上新建库表进行分析，可将上一步收集的信息文件，整个 output 目录取回到测试环境分析。
4. 根据工具生成的文件 `output/information_by_cl.csv` 和 `output/information_by_cltype.csv`，人为进一步分析确定需要迁移的表（后续会优化为自动分析出迁移目标）。

### 扩建新分区表场景
1. 配置 args.js 中变量
2. 收集影像平台快照信息

	```
	sdb -e 'var MODE = "collect"' -f check_model.js
	```

3. 分析收集到的信息

	```
	sdb -e 'var MODE = "run"' -f check_model.js
	```

	如果不允许在生产环境上新建库表进行分析，可将上一步收集的信息文件，整个 output 目录取回到测试环境分析。

4. 根据实际需求，使用工具 bin/create_domain.js 新建域 
	- 测试新建域的信息
		```
		sdb -e 'var MODE = "test"' -f create_domain.js
		```

    - 新建域
		```
		sdb -e 'var MODE = "run"' -f create_domain.js
		```

    - 如果建域过程中出现问题需要回滚（仅能回滚空域）
		```
		sdb -e 'var MODE = "rollback"' -f create_domain.js
		```
5. 根据实际需求，修改生成文件 `conf/add_cl.csv` 中建表的配置，比如
	- 只想扩建部分表，删除其他表信息
	- 修改扩建表的配置
6. 修改 conf/config.json 配置文件，确定新建表及索引的通用配置参数
7. 使用工具 bin/add_cl.js 扩建 CL
	- 测试扩建表的信息
		```
		sdb -e 'var MODE = "test"' -f add_cl.js
		```

	- 扩建表
		```
		sdb -e 'var MODE = "run"' -f add_cl.js
		```

	- 如果建表过程中出现问题需要回滚（仅能回滚空 CL 和空 CS）
		```
		sdb -e 'var MODE = "rollback"' -f add_cl.js
		```


### 新建业务应用场景
1. 配置 args.js 中变量
2. 根据实际需求，使用工具 bin/create_domain.js 新建域
	- 测试新建域的信息
		```
		sdb -e 'var MODE = "test"' -f create_domain.js
		```

    - 新建域
		```
		sdb -e 'var MODE = "run"' -f create_domain.js
		```
    - 如果建域过程中出现问题需要回滚（仅能回滚空域）
		```
		sdb -e 'var MODE = "rollback"' -f create_domain.js
		```
5. 根据实际需求，修改生成文件 `conf/add_cl.csv` 中建表的配置，比如
	- 只想扩建部分表，删除其他表信息
	- 修改扩建表的配置
6. 修改 conf/config.json 配置文件，确定新建表及索引的通用配置参数
7. 使用工具 bin/add_cl.js 新建 CL
	-  测试新建应用的表信息
		```
		sdb -e 'var MODE = "test"' -f add_cl.js
		```

	- 新建应用表
		```
		sdb -e 'var MODE = "run"' -f add_cl.js
		```

	- 如果建表过程中出现问题需要回滚（仅能回滚空 CL 和空 CS）
		```
		sdb -e 'var MODE = "rollback"' -f add_cl.js
		```

