from copy import deepcopy
import pandas as pd
import os
from datetime import time

#简单事务操作
begin=0 #全局变量默认为0
commit_or_rollback=0
temp=pd.DataFrame()#用于备份
original=pd.DataFrame()#用于备份
which_table=''
op_type=''
old_time=''#

pd.set_option('display.max_columns', None)# 显示所有行
pd.set_option('display.max_rows', None)# 设置value的显示长度为100，默认为50
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 5000)


def error(info=None):
    if info:
        print( info)


def in_list(key, df):
    for item in df.values.tolist():
        if key == item:
            return True
    return False


def get_now_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def insert(table_name, data, unique_columns=None, unique_values=None):
    df = pd.read_csv(f'./data/{table_name}.csv', encoding='utf8')
    if unique_columns:
        temp_df = df.copy()
        for key, value in zip(unique_columns, unique_values):#zip将多个对象中的元素打包成元组（列名，值）
            temp_df = temp_df[temp_df[key] == value]
        idx = temp_df.index
        if len(idx) > 0:#说明在插入之前，表中unique这列已有跟我们要插入的数据取值一样的，但这列需要满足unique，所以报错
            return False
    # 事务操作，先将要操作的表备份
    global begin,temp,which_table,original,op_type
    if begin:
        temp = deepcopy(df)
        original = deepcopy(df)
        which_table=table_name
        op_type='insert'
        #将其插入先插入temp
        temp.loc[len(temp)] = data  # (在末尾插入数据，一整行）
    else:
        df.loc[len(df)] = data#(在末尾插入数据，一整行）
        df.to_csv(f'./data/{table_name}.csv', index=False)
    return True


def select(table_name, raw_attributes, where_key=None, where_value=None):
    df = pd.read_csv(f'./data/{table_name}.csv', encoding='utf8')
    if table_name == 'schema':
        df.set_index('db_name', inplace=True, drop=False)
        #DataFrame.set_index(keys, drop=True, append=False, inplace=False, verify_integrity=False)，是给dataframe数据结构设索引，这样查询啥的快
        # key需要设置为索引的列，drop默认为True，删除用作新索引的列,inplace：输入布尔值，表示当前操作是否对原数据生效，默认为False,verify_integrity：检查新索引的副本。否则，请将检查推迟到必要时进行。将其设置为false将提高该方法的性能，默认为false
    if table_name == 'columns':
        df.set_index(['table_schema', 'table_name'], inplace=True, drop=False)#这并不能唯一标识一行啊，得再加上一行column
    if table_name == 'index':
        df.set_index(['table_schema', 'table_name', 'index_name'], inplace=True, drop=False)
    df = df.applymap(lambda x: str(x))#applymap()对DataFrame中的每个单元格执行指定函数的操作,str()转为字符串
    attributes = df.columns if raw_attributes == ['*'] else raw_attributes#想要得到的列
    if where_key:
        for key, value in zip(where_key, where_value):
            df = df[df[key].str.lower() == value]#这就是dataframe的查询操作，查询条件（一般是列名-值）
        return df[attributes]#选取attributes那些列
    else:
        return df[attributes]


def delete(table_name, key=None, value=None):
    global begin,temp,which_table,original,op_type
    if key:
        df = pd.read_csv(f'./data/{table_name}.csv', encoding='utf8')
        if isinstance(key, str):#isinstance() 函数来判断一个对象是否是一个已知的类型,
            idx = df.loc[(df[key] == value)].index
            if len(idx) == 0:#要删除的行不存在
                return False
            #事务操作，先备份
            if begin:
                temp=deepcopy(df)
                original= deepcopy(df)
                which_table = table_name
                op_type='delete'
               #先不删实际的，先从备份中删
                temp=temp.drop(index=idx)
            else:
                df = df.drop(index=idx)#drop是series类型自带的函数，删除index那行
        else:#删除条件为多个DELETE FROM Websites WHERE name='Facebook' AND country='USA'
            exist = False
            df_len = len(df)#dataframe的行数
            # 事务操作，先备份
            if begin:
                temp = deepcopy(df)
                original = deepcopy(df)
                which_table = table_name
                op_type = 'delete'
            for i in range(df_len):#range(10)表示从0到9不包括10
                skip = False
                for k, v in zip(key, value):
                    if df.loc[i, k] != v:#第i行第k列不等于v，则跳过
                        skip = True
                        break#跳出本层循环
                if not skip:
                    if begin:
                        # 先不删实际的，先从备份中删
                        temp = temp.drop(index=i)
                        op_type = 'delete'
                    else:
                        df = df.drop(index=i)#drop是series类型自带的函数，删除index那行
                    exist = True
            if not exist:
                return False
        if begin==0:#没事务，写回
            df.to_csv(f'./data/{table_name}.csv', index=False)#写回到文件中
    else:
        os.remove(f"./data/{table_name}.csv")#删除文件，即没有指定删除条件，即删除整个table
    return True


# UPDATE table_name SET column1 = value1, column2 = value2 WHERE condition(如id=1);
def update(table_name, column_name, column_value, where_key, where_value):
    global begin,temp,which_table,original,op_type
    df = pd.read_csv(f'./data/{table_name}.csv', encoding='utf8')
    if isinstance(where_key, str) :#isinstance() 函数来判断一个对象是否是一个已知的类型,
        idx = df.loc[df[where_key] == where_value].index#读取满足条件的那一行
    elif len(where_key)==1:
        #将list转为str:
        where_key=where_key[0]
        where_value=where_value[0]
        idx = df.loc[df[where_key] == where_value].index  # 读取满足条件的那一行
    else:#指定了多个key和value的情况
        tmp_df = df.copy()
        for k, v in zip(where_key, where_value):#选择要更新的行
            tmp_df = tmp_df[tmp_df[k] == v]
        idx = tmp_df.index#但是可能有多行需要更新，
    if len(idx) == 0:#如果没能找到要更新的行，则报错
        return False

    # 事务操作，先备份
    if begin==1 and commit_or_rollback==0:#注意只有这里需要，因为commit和rollback需要更新tables表
        temp = deepcopy(df)
        original= deepcopy(df)
        which_table = table_name
        op_type = 'update'
        #可能有多列需要更新
        if len(idx)>1:
            for i in range(0,len(idx)-1):
                temp.loc[i, column_name] = column_value#不更新实际的值，而是更新tenp
        else:
            temp.loc[idx,column_name] = column_value
    else:
        # 可能有多列需要更新
        if len(idx)>1:
            for i in range(0, len(idx) - 1):
                df.loc[i, column_name] = column_value
        else:
            df.loc[idx, column_name] = column_value#更新某行某列的值
        df.to_csv(f'./data/{table_name}.csv', index=False)#写到文件中
    return True

 #CREATE DATABASE <数据库名> [CHARACTER SET <字符集名>]  [COLLATE <校对规则名>];
def proc_create_db(tokens):
    db_name = tokens[2]
    try:
        charset_name = tokens.index('set')#index()：查找字串在原字符串中首次出现的位置，返回子串首个元素的位置，
    except ValueError:
        charset_name = 'utf8'#没找到默认utf8编码
    try:
        collate_name = tokens.index('collate')
    except ValueError:
        collate_name = 'utf8_general_ci'
    data = [db_name, charset_name, collate_name]
    ret =insert('schema', data, unique_columns=['db_name'], unique_values=[db_name])
    if not ret:
        error(f"数据库[{db_name}]已存在!")
    else:
        print(f"数据库{db_name}创建成功！")



def proc_create_table(db_name, tokens):
    table_name = tokens[2]
    item_list = []
    stack = []
    i = tokens.index('(')#从(开始是各列的定义
    stack.append('(')
    column_pos = 0
    last_k = len(tokens) - 2 - tokens[::-1].index(')')#[::-1]： token倒序为新token，新token中第一个)的位置，也就是列的定义和主键外键啥的结束的位置
    key_name = ''
    key_type = ''
    auto_increment_idx = ''
    columns=[]#所有列
    #最初i从(列的定义开始，column_pos=0
    while len(stack) > 0:#stack.append('(')
        i += 1
        if tokens[i] == ')':#列的定义结束了
            if stack[-1] == '(':#-1表示列表中最后一个元素
                stack.pop()#移除列表的最后一个元素，然后len(stack)<=0，while循环就结束了
            else:
                error("无效的sql语句")
                break
        else:
            column_pos += 1

            if tokens[i] in ['primary']:#主键:primary key (id)，这一般在最后
                key_name = tokens[tokens.index('primary') + 3]#+3是因为primary key (
                key_type = 'primary'
                while tokens[i] != ',' and i < last_k:
                    i += 1
            else:
                default_value = ''
                is_nullable = True
                key_len = ''
                auto_increment = False
                #以上是默认配置，要看接下来在语句中有没有识别到相应元素
                column_name = tokens[i]
                columns.append(column_name)#也是个list
                i += 1
                key_type = tokens[i]
                i += 1
                if tokens[i] == '(':
                    key_len = tokens[i + 1]#像varchar(45)和int(11)
                    i += 3
                if tokens[i] == 'not' and tokens[i + 1] == 'null':#是否非空
                    is_nullable = False
                    i += 2
                if 'auto_increment' in tokens[i:]:#[i:]从第i个元素一直取到最后
                    auto_increment = True
                    idx = tokens.index('auto_increment')#自增长可以设置初始值
                    if tokens[idx + 1] == '=':
                        auto_increment_idx = tokens[idx + 2]
                if 'default' in tokens[i:]:
                    default_value = tokens[tokens.index('default') + 1]
                if auto_increment and auto_increment_idx == '':
                    auto_increment_idx = 0#没设置初始值的话就默认从0自增长
                item_list.append(
                    [column_name, column_pos, default_value, is_nullable, key_type, key_len, auto_increment])
                while tokens[i] != ',' and i < last_k:#跳出循环，回到上面按相同的步骤处理下一列
                    i += 1
    #判断该数据库内是否已存在同名的表
    head_data = [db_name, table_name]
    df = select('columns', raw_attributes=['table_schema', 'table_name'])

    if in_list(head_data, df):
        error(f"数据库 [{db_name}]中表 [{table_name}]已存在!")
        return

    try:
        engine = tokens[tokens.index('engine') + 2]
    except ValueError:
        engine = 'innodb'#数据库引擎默认是innodb
    try:
        charset = tokens[tokens.index('charset') + 2]
    except ValueError:
        print("charset=", charset)
        charset = 'utf8'#字符集默认是utf8
    try:
        table_type = tokens[tokens.index('table_type') + 2]
    except ValueError:
        table_type = 'base_table'

#向table表中插入表数据
    insert('tables',
           [db_name, table_name, table_type, engine, 0, get_now_time(), auto_increment_idx, get_now_time(), charset])#0是table_rows，excel中最后一栏是table_collation校对规则，excel中错了吧
    for item in item_list:#item_list是[column_name, column_pos, deafault_value, is_nullable, key_type, key_len, auto_increment]
        data = head_data + item #head_data = [db_name, table_name]
        if item[0] == key_name:
            data.append(key_type)#是否为主键，外键啥的,data是个list
            # 向index表插入索引数据
            insert('index', [db_name, table_name, False, key_type, key_name, data[-5], 'btree'])#false是non_unique
        else:
            data.append('')

#向column表插入列数据
        insert('columns', data)
#真的建表
    real_table=pd.DataFrame(data=None,columns=columns)
    real_table.to_csv(f'./data/{table_name}.csv', index=False,mode='w')#写模式：覆盖，可以不存在，会创建新文件
    print(f"创建表{table_name}成功")

#INSERT INTO table_name values(a,b,c,d)
def proc_insert_data(db_name, tokens, insert_op=True):#proc_delete_data()内调用时insert_op=flase
    root = tokens[0] == "sudo"#是否用了sudo，如果有sudo则token[3]才是table_name
    table_name = tokens[3] if root else tokens[2]
    old_row_time = select('tables', ['table_rows', 'update_time'], where_key=['table_schema', 'table_name'],#table_schema就是表所在的数据库
                          where_value=[db_name, table_name])
    if len(old_row_time) == 0:
        error(f'数据库 [{db_name}]中表[{table_name}] 不存在!')
        return
    if root:#有sudo，会对真正的数据文件进行操作，反之，将只更新当前数据库运行时的信息
        if insert_op:
            # really insert data into tables
            vid = tokens.index('values')#第一次出现value的位置
            vid += 2#为什么加2，因为values(
            values = tokens[vid:-2]#从vid的位置开始去掉最后两个字符，也就是)和;
            values = [e for e in values if e not in [',']]#去掉值之间的分隔符（逗号）
            insert(table_name, values)
            print("插入数据成功")
        else:#删除数据
            where_data = None
            where_idx = len(tokens)
            if 'where' in tokens:#即有条件删除,否则是无条件即删除整个表
                where_idx = tokens.index('where')#delete from table4 where id=1
                where_data = tokens[1 + where_idx:-1]#从where_idx+1开始到倒数第一个（不含）因为s[i:j] 表示获取a[i]到a[j-1]，
            from_table = tokens[1 + tokens.index('from'):where_idx]
            where_keys = []
            where_values = []
            if where_data:#有条件删除可能有多个条件
                where_data = "".join(where_data)#join()拼接生成新的字符串
                for wd in where_data.split("and"):#通过and拆分多个条件
                    wd = re.sub('r[\"\']', '', wd.replace('\'', ''))
                    #re.sub()正则表达式的替换，三个必选参数：pattern正则表达式, repl处理为什么, string要被处理的字符串
                    #relace(old,new)，字符串中的 old（旧字符串） 替换成 new(新字符串)
                    key = wd.split("=")[0]#得到的是第一个=之前的内容
                    value = wd.split("=")[1]#得到的是第一个=和第二个=之间的内容
                    where_keys.append(key)
                    where_values.append(value)
            delete(table_name, key=where_keys, value=where_values)
            print("删除数据成功")
    if begin==0:
         # def update(table_name, key, key_value, new_value):
        dt = 1 if insert_op else -1
        update('tables', column_name='table_rows', column_value=int(old_row_time['table_rows']) + dt,
            where_key='table_name',where_value=table_name)#表行数加一
        update('tables', column_name='update_time', column_value=get_now_time(), where_key='table_name',
            where_value=table_name)


#UPDATE table_name SET column1 = value1, column2 = value2, ... WHERE condition1 and condition2 and ...
def proc_update_data(db_name, tokens):
    #更改表中数据
    set_idx= tokens.index('set')
    where_idx = tokens.index('where')
    where_column=tokens[set_idx+1:where_idx ]
    where_columns = []
    where_datas = []
    #得到要更新的列，和新值
    if where_column:
        where_column = "".join(where_column)
        for wd in where_column.split(","):  # 用,分割
            wd = re.sub('r[\"\']', '', wd.replace('\'', ''))
            # re.sub()正则表达式的替换，三个必选参数：pattern正则表达式, repl处理为什么, string要被处理的字符串
            # relace(old,new)，字符串中的 old（旧字符串） 替换成 new(新字符串)
            column= wd.split("=")[0]  # 得到的是第一个=之前的内容
            data = wd.split("=")[1]  # 得到的是第一个=和第二个=之间的内容
            where_columns.append(column)
            where_datas.append(data)
    # 更新条件，得到符合条件的那几行
    where_data = tokens[1 + where_idx:-1]  # 所有查询条件，从where+1的位置到倒数第一个（不含），语句的倒数第一个是分号
    where_keys = []
    where_values = []
    if where_data:
        where_data = "".join(where_data)
        for wd in where_data.split("and"):  # 用and分割
            wd = re.sub('r[\"\']', '', wd.replace('\'', ''))
            # re.sub()正则表达式的替换，三个必选参数：pattern正则表达式, repl处理为什么, string要被处理的字符串
            # relace(old,new)，字符串中的 old（旧字符串） 替换成 new(新字符串)
            key = wd.split("=")[0]  # 得到的是第一个=之前的内容
            value = wd.split("=")[1]  # 得到的是第一个=和第二个=之间的内容
            where_keys.append(key)
            where_values.append(value)
  #开始更新：
    table_name = tokens[tokens.index('update') + 1]#更新真正的表
    if update(table_name, column_name=where_columns, column_value=where_datas, where_key=where_keys,
           where_value=where_values):
        print("更新数据成功")
    else:
        print("更新数据失败")
    if begin==0:
        # 修改更新时间为当前时刻
        ret = update('tables', column_name='update_time', column_value=get_now_time(), where_key='table_name',
                 where_value=table_name)
        if not ret:
            error(f'数据库 [{db_name}] 中表[{table_name}] 不存在!')
            return


#DELETE FROM runoob_tbl WHERE runoob_id=3;
def proc_delete_data(db_name, tokens):#删除行，转到insert中处理
    proc_insert_data(db_name=db_name, tokens=tokens, insert_op=False)


#删除字段alter table tablename drop (column);
def proc_drop(db_name, tokens):
    table_name = tokens[tokens.index('table') + 1]
    column_name = tokens[tokens.index('drop') + 2]#+2是因为drop(
    ret = delete(table_name='columns', key=['table_name', 'column_name'], value=[table_name, column_name])
    if not ret:
        error(f'数据库[{db_name}] 中表 [{table_name}]列 [{column_name}] 不存在!')
        return
    #从真正的表删除
    table = pd.read_csv(f'./data/{table_name}.csv', encoding='utf8')
    table=table.drop(column_name,axis=1)#axis=1表示列
    # 写回
    table.to_csv(f'./data/{table_name}.csv', index=False)
    print("删除字段成功")



#sudo alter table book add column 校区 varchar(15) default 将军路;
# 修改和删除字段的默认值
def proc_add_column(db_name, tokens):
    table_name = tokens[tokens.index('table') + 1]
    i = tokens.index('add') + 2#列名
    column_name = tokens[i]
    column_type = tokens[i + 1]
    if tokens[i+2] == '(':
        max_char_length  = tokens[i + 3]  # 像varchar(45)和int(11)
        i += 5#这种情况下i+5的位置是第一个属性
    else:
        i += 2#像float
    #处理要添加的列的各项属性
    default_value = ''
    nullable = True
    auto_increment = False
    column_key = ''#是否为主键
    auto_increment_idx = ''
    if tokens[i] == 'not' and tokens[i + 1] == 'null':
        nullable = False
        i += 2

    if 'auto_increment' in tokens:
        auto_increment = True
        idx = tokens.index('auto_increment')
        if tokens[idx + 1] == '=':
            auto_increment_idx = tokens[idx + 2]#自增长初始值

    if 'default' in tokens:
        default_value = tokens[tokens.index('default') + 1]
   #看目前表中有几列，在此基础上加新的一列
    last_ordinal_pos = select('columns', 'ordinal_position', where_key=['table_schema', 'table_name'],
                              where_value=[db_name, table_name]).max()
    data = [db_name, table_name, column_name, float(last_ordinal_pos) + 1, default_value, nullable, column_type,
            max_char_length,
            auto_increment, column_key]
    insert('columns', data)

    #真的表中添加字段，若有默认值，记得赋值，若自增长，记得赋值
    table=pd.read_csv(f'./data/{table_name}.csv', encoding='utf8')
    #table=pd.concat([table,pd.DataFrame(columns=column_name)],sort=False)#shape[1]：读取列数
    table[column_name] = None
    if auto_increment:
        for i in range(auto_increment_idx,table.shape[0]):#shape[0]读取行数
            table[column_name]=i
    if 'default' in tokens:
        table[column_name]=default_value
    #写回
    table.to_csv(f'./data/{table_name}.csv',index=False)
    print("添加字段成功")



#添加某列为索引
# alter table table_name add index [index_name] 列名;
# alter table table_name add unique 列名;
# alter table table_name add primary key 列名;
def proc_add_key(db_name, tokens):
    table_name = tokens[tokens.index('table') + 1]
    i = tokens.index('add') + 1
    idx = tokens.index(';')
    column_name = tokens[idx - 1]
    nullable = select('columns', raw_attributes=['nullable'],
                      where_key=['table_schema', 'table_name', 'column_name'],
                      where_value=[db_name, table_name, column_name]).values[0, 0]#返回字典中的第一个键值对(key value)

    if tokens[i] in ['index', 'unique']:
        non_unique = False if tokens[i] == 'unique' else True
        i += 1
        index_name = tokens[i]#没指定索引名的话就直接拿列名当索引
        index_type = 'btree'
        i += 1
        if tokens[i] != '(':#说明指定了索引类型，不再是默认的btree，否则索引名后应是列名
            index_type = tokens[i]
            i += 1
        #     table_schema,table_name,non_unique,index_name,column_name,nullable,index_type
        ret = insert('index', [db_name, table_name, non_unique, index_name, column_name, nullable, index_type],
                     unique_columns=['table_schema', 'index_name', 'table_name'],
                     unique_values=[db_name, index_name, table_name])
        if not ret:
            error(f'数据库 [{db_name}] 中索引 [{index_name}] 已存在!')
            return

    elif tokens[i] == 'primary':
        i += 2
        index_type = 'btree'
        if tokens[i] != '(':
            index_type = tokens[i]
            i += 1
        ret = insert('index', [db_name, table_name, False, 'primary', column_name, nullable, index_type],
                     unique_columns=['table_schema', 'index_name', 'table_name'],
                     unique_values=[db_name, 'primary', table_name])
        if not ret:
            error(f'数据库 [{db_name}] 中索引 [PRIMARY] 已存在!')
            return

    print("添加索引成功")



#可同时修改列名和列的数据类型：alter table 表名 change  旧列名  新列名 新的数据类型
# #sudo alter table book change 校区 campus;
#alter table book modify column名 datatype [default value][null/not null],….);
def proc_change(db_name, tokens):
    table_name = tokens[tokens.index('table') + 1]
    if 'change' in tokens:
        column_name = tokens[tokens.index('change') + 1]
        new_column_name = tokens[tokens.index('change') + 2]
        i = tokens.index('change') + 3
    if 'modify' in tokens:
        column_name=tokens[tokens.index('modify')+1]
        i=tokens.index('modify')+2

    if tokens[i] !=';':#说明修改了数据类型，如varchar(20)
        column_type = tokens[i]#其实应该判断一下源数据类型和新数据类类型的差别，差别大的表中数据无法存储，应报错
        update('columns', column_name='data_type', column_value=column_type,
               where_key=['table_schema', 'table_name', 'column_name'], where_value=[db_name, table_name, column_name])
        i += 2
    max_len = ''
    if '(' in tokens:
        max_len = tokens[i]
        i += 1
        update('columns', column_name='max_char_length', column_value=max_len,
           where_key=['table_schema', 'table_name', 'column_name'], where_value=[db_name, table_name, column_name])
    #如果还没到指令尾，说明修改了列的属性，那要是我没修改数据类型只修改了属性呢，没见过这样的情况
    if i + 1 < len(tokens) and [i] == 'not' and tokens[i + 1] == 'null':
        nullable = False
    if 'auto_increment' in tokens:
        auto_increment = True
        update('columns', column_name='auto_increment', column_value=auto_increment,
               where_key=['table_schema', 'table_name', 'column_name'], where_value=[db_name, table_name, column_name])
    if 'default' in tokens:
        default_value = tokens[tokens.index('default') + 1]
        update('columns', column_name='default_value', column_value=default_value,
               where_key=['table_schema', 'table_name', 'column_name'], where_value=[db_name, table_name, column_name])
#修改列名
    if 'change' in tokens:
        ret = update('columns', column_name='column_name', column_value=new_column_name,
                 where_key=['table_schema', 'table_name', 'column_name'],
                 where_value=[db_name, table_name, column_name])
        if not ret:
            error(f'数据库[{db_name}] 中表 [{table_name}] 列 [{column_name}] 不存在!')
            return
            # 修改真正的表
        table = pd.read_csv(f'./data/{table_name}.csv', encoding='utf8')
        colNameDict = {column_name: new_column_name}  # 将‘源数据列名’改为‘新列名’
        table.rename(columns=colNameDict, inplace=True)
        # 写回
        table.to_csv(f'./data/{table_name}.csv', index=False)
    print("修改字段成功")



#修改列的数据类型，或其他属性alter table book modify column名 datatype [default value][null/not null],….);
def proc_modify(db_name, tokens):
    proc_change(db_name, tokens)#转到change去处理


#删除索引，分为普通索引，主键索引，唯一索引#ALTER  TABLE  table_name   DROP  INDEX  index_name;
def proc_drop_key(db_name, tokens):
    table_name = tokens[2]
    index_name = ''
    if 'index' in tokens:
        index_name = tokens[tokens.index('index') + 1]
    elif 'primary' in tokens:#主键会自动建一个唯一索引的请况alter table table_name drop primary key ;
        index_name = 'primary'
    ret = delete('index', key=['table_schema', 'table_name', 'index_name'], value=[db_name, table_name, index_name])
    if not ret:
        error(f'数据库 [{db_name}] 中表 [{table_name}] 索引 [{index_name}] 不存在!')
        return
    print("删除索引成功")


#删除索引，处理drop index index_name on table_name这种形式
def proc_drop_key2(db_name,tokens):
    table_name = tokens[tokens.index('on') + 1]
    #不知索引名是否要份情况处理
    index_name = tokens[tokens.index('index') + 1]#不知是否有drop primary on table_name这种形式
    ret = delete('index', key=['table_schema', 'table_name', 'index_name'], value=[db_name, table_name, index_name])
    if not ret:
        error(f'数据库 [{db_name}] 中表 [{table_name}] 索引 [{index_name}] 不存在!')
        return
    print("删除索引成功")



#ALTER TABLE 语句用于在已有的表中添加、删除或修改列。
def proc_alter(db_name, tokens):
    # drop
    if 'drop' in tokens and 'column' in tokens:#删除列
        proc_drop(db_name, tokens)
    elif 'drop' in tokens:
        proc_drop_key(db_name, tokens)#删除索引
    elif 'add' in tokens and 'column' in tokens:#添加列
        proc_add_column(db_name, tokens)
    elif 'add' in tokens:
        proc_add_key(db_name, tokens)#添加suoyin
    elif 'change' in tokens:
        proc_change(db_name, tokens)#更改字段名称alter table sq_communitymanage change  city  cityname VARCHAR(20);
    elif 'modify' in tokens:
        proc_modify(db_name, tokens)#修改列的属性alter table test_info modify column_name VARCHAR(1000);


#删除数据库DROP DATABASE [IF EXISTS] database_name;
def proc_drop_db(tokens):
    db_name = tokens[2]
    ret = delete('schema', key='db_name', value=db_name)
    if not ret:
        error(f'数据库 [{db_name}] 不存在!')
        return
    delete('columns', key='table_schema', value=db_name)
    delete('tables', key='table_schema', value=db_name)
    delete('index', key='table_schema', value=db_name)
    #删除真正的表和其中数据
    list = select('tables', 'table_name', where_key='table_schema',
                 where_value=db_name)
    for table_name in list:
        os.remove(f"./data/{table_name}.csv")
    print(f"删除数据库{db_name}成功")


#删除表sudo drop table book;
def proc_drop_table(db_name, tokens):
    table_name = tokens[tokens.index('table')+1]
    ret = delete('tables', key=['table_schema', 'table_name'], value=[db_name, table_name])
    if not ret:
        error(f'数据库[{db_name}] 中表 [{table_name}] 不存在!')
        return
    delete('columns', key=['table_schema', 'table_name'], value=[db_name, table_name])
    delete('index', key=['table_schema', 'table_name'], value=[db_name, table_name])
    #删除真正的表和其中数据
    os.remove(f"./data/{table_name}.csv")
    print(f"删除表{table_name}成功")


import re
#select column_name, data_type from columns表名 where table_name='tasks' and column_name='subject'
def proc_select_data(db_name, tokens):
    from_idx = tokens.index('from')
    attributes = tokens[1:from_idx]#查询最终要得到哪几列的值
    attributes = [e for e in attributes if e != ',']#去掉多列之间分隔的逗号
    where_data = None
    where_idx = len(tokens)
    if 'where' in tokens:#查询条件，得到符合条件的那几行
        where_idx = tokens.index('where')
        where_data = tokens[1 + where_idx:-1]#所有查询条件，从where+1的位置到倒数第一个（不含），语句的倒数第一个是分号
    from_table = tokens[1 + from_idx:where_idx]
    where_keys = []
    where_values = []
    if where_data:
        where_data = "".join(where_data)
        for wd in where_data.split("and"):#用and分割
            wd = re.sub('r[\"\']', '', wd.replace('\'', ''))
            # re.sub()正则表达式的替换，三个必选参数：pattern正则表达式, repl处理为什么, string要被处理的字符串
            # relace(old,new)，字符串中的 old（旧字符串） 替换成 new(新字符串)
            key = wd.split("=")[0]#得到的是第一个=之前的内容
            value = wd.split("=")[1]#得到的是第一个=和第二个=之间的内容
            where_keys.append(key)
            where_values.append(value)
    data = select(from_table[0], raw_attributes=attributes, where_key=where_keys, where_value=where_values)
    if len(data) == 0:#没有符合条件的
        print('未找到符合条件的数据')
    else:
        print(data)


def select_by_schema(table_name, db_name):#index optimization
    df = pd.read_csv(f'./data/{table_name}.csv', encoding='utf8')
    df.set_index('table_schema', drop=False, inplace=True)#跟select（）相比设的索引不一样，但在table,index和column表里所有行的table_schema都一样啊
    try:
        data = df.loc[db_name]#读取db_name相等的那一行，excel中db_name就是table_schema
        return data
    except KeyError:
        return None


def proc_show(db_name, tokens):
    if tokens[1] == 'databases':
        parse(f'select db_name from schema;')
    elif tokens[1] in ['tables', 'columns', 'index']:
        ans = select_by_schema(tokens[1], db_name)
        print(ans)



db_name = ''

#解析输入的创建数据库的语句
def parse(cmd: str):#指定参数类型为str
    import re#正则表达式模块
    tokens = re.split(r"([ ,();=])", cmd.lower().strip())
    #split()切分字符串re.split(pattern, string)，pattern表示用于指定分割规则的正则表达式，string被切分的字符串，strip() 方法用于移除字符串头尾指定的字符（默认为空格或换行符）或字符序列。
    tokens = [t for t in tokens if t not in [' ', '', '\n']]
    global db_name
    global begin,which_table, op_type, temp,commit_or_rollback#事务全局变量
    i = 0
    if tokens[0] == "sudo": i += 1#添加了 sudo，会对真正的数据文件进行操作，反之，将只更新当前数据库运行时的信息
    if tokens[i] == 'use':
        db_name = tokens[i + 1]#使用前必须要说用哪个数据库，先去模式表里找有没有要用的的数据库
        ret = select('schema', ['*'], where_key=['db_name'], where_value=[db_name])
        if len(ret) == 0:
            error(f"数据库 [{db_name}] 不存在")
            return
        print(f'使用数据库{db_name}')#提示用户开始对该数据库做操作

    #事务操作事务用来管理 insert、update、delete 语句
    if tokens[i] == 'begin':
        #打开记录开关，
        begin=1
    if tokens[i] == 'commit':
        commit_or_rollback=1
        #把temp写回到对应表中,用覆盖的方式
        temp.to_csv(f'./data/{which_table}.csv', index=False)#to_csv()参数mode写模式，默认w 只能写, 可以不存在, 必会擦掉原有内容从头写
        #同时根据分insert, delete，update更新四个内部表结构
        if op_type in ['insert' or 'delete']:
            dt = 1 if op_type=='insert' else  -1
            old = select('tables', ['table_rows', 'update_time'], where_key=['table_schema', 'table_name'],
                                  where_value=[db_name, which_table])
            global old_time
            old_time=old['update_time']
            update('tables', column_name='table_rows', column_value=int(old['table_rows']) + dt,
                   where_key='table_name', where_value=which_table)  # 表行数加一
            update('tables', column_name='update_time', column_value=get_now_time(), where_key='table_name',
                   where_value=which_table)#修改更新时间
        if op_type=='update':
            update('tables', column_name='update_time', column_value=get_now_time(), where_key='table_name',
                         where_value=which_table)#修改更新时间
    if tokens[i] == 'rollback':
        commit_or_rollback=1
        original.to_csv(f'./data/{which_table}.csv', index=False)  # to_csv()参数mode写模式，默认w 只能写, 可以不存在, 必会擦掉原有内容从头写
        # 做与commit相反的操作
        if op_type in ['insert' or 'delete']:
            dt = -1 if op_type == 'insert' else 1
            old = select('tables', ['table_rows', 'update_time'], where_key=['table_schema', 'table_name'],
                         where_value=[db_name, which_table])
            update('tables', column_name='table_rows', column_value=int(old['table_rows']) + dt,
                   where_key='table_name', where_value=which_table)  # 表行数
            update('tables', column_name='update_time', column_value=old_time, where_key='table_name',
                   where_value=which_table)  # 恢复更新时间
        if op_type=='update':
            update('tables', column_name='update_time', column_value=old_time, where_key='table_name',
                   where_value=which_table)  # 恢复更新时间
    if tokens[i] == 'end':
        begin=0

    if tokens[i] == 'create' and tokens[i + 1] == 'database':
        proc_create_db(tokens)
    if tokens[i] == 'create' and tokens[i + 1] == 'table':
        proc_create_table(db_name, tokens)
    if tokens[i] == 'insert':
        proc_insert_data(db_name, tokens)
    if tokens[i] == 'update':
        proc_update_data(db_name, tokens)#update table4 set X=0 where id=1
    if tokens[i] == 'delete':
        proc_delete_data(db_name, tokens)#delete删除的是 数据，drop删除的是表；
    if tokens[i] == 'alter':
        proc_alter(db_name, tokens)
    if tokens[i] == 'drop' and tokens[i + 1] == 'database':
        proc_drop_db(tokens)
    if tokens[i] == 'drop' and tokens[i + 1] == 'table':
        proc_drop_table(db_name, tokens)
    if tokens[i] == 'drop' and tokens[i + 1] == 'index':
        proc_drop_key2(db_name, tokens)
    if tokens[i] == 'select':
        proc_select_data(db_name, tokens)#show database时会变为select db_name from schema
    if tokens[i] == 'show':
        proc_show(db_name, tokens)#show database,tables,index,columns
    if tokens[i] == 'database':
        print(db_name)#用于显示当前使用的数据库

    if begin:
        #记录操作到transactional表中
        record = pd.read_csv(f'./data/transactional.csv', encoding='utf8')
        c_type=''
        if 'insert' in tokens or 'update' in tokens:
            c_type='SET'
        if 'delete' in tokens:
            c_type='UNSET'
        command = "".join(tokens)
        data = [get_now_time(), which_table, c_type,command]
        record.loc[len(record)] = data
        record.to_csv(f'./data/transactional.csv', index=False)

import time

if __name__ == '__main__':
    cmd_list = []

    while True:
        cmd = input("command?  ")
        if cmd == 'exit;':
            break
        cmd_list.append(cmd)
        while ';' not in cmd:#即使换行了也没有分号
            cmd = input('> ')
            cmd_list.append(cmd)
        cmd = " ".join(cmd_list)#将字符串、元组、列表中的元素以指定的字符(分隔符)连接生成一个新的字符串，sep'.join(要連接的元素序列)，sep：分隔符。可以为空
        cmd_list = []
        st = time.time()#返回当前时间的时间戳（1970纪元后经过的浮点秒数）
        parse(cmd)#解析输入的创建数据库的语句

