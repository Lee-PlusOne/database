# 准备需要的包
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestRegressor

def DataCompletion(datapath,outpath):
    data = pd.read_csv(datapath)  # 导入原始数据
    #由于x,y,z,w每一列都有缺失，所以再创建一列都是1的完整数据并填写名称
    data['temp']=1
    data=data[['temp','X','Y','Z','W']]
    data.to_csv(datapath)
    #再次读取
    data = pd.read_csv(datapath)
    target = data['temp']
    # 这里的‘id’是指没有数据缺失的那一列的名称
    features = data.iloc[:,1:]  # 我们从第二列取到最后一列，也就是x,y,z,w


    X_full, y_full = features, target
    n_samples = X_full.shape[0]  # 样本，shape函数是Numpy中的函数，它的功能是读取矩阵的长度，比如shape[0]就是读取矩阵第一维度的长度，即行数。
    n_features = X_full.shape[1]  # 特征， shape[1]就是矩阵的列数
    print('行数:',n_samples)
    print('列数:',n_features-1)#-1是因为temp是后加的


    X_missing_reg = X_full.copy()
    # 查看缺失情况
    missing = X_missing_reg.isna().sum()
    #dataframe的isna()判断数字数组中的值是否为非数字NaN,用来检测缺失值，它返回一个相同大小的dataframe，其中NA值(例如None或numpy.NaN)被映射为True值。其他所有内容都映射为False值
    #dataframe.sum()用来默认对所有列求和，返回一个Series（相当于二维数组），所以missing里面存的是每列的缺失值个数
    missing = pd.DataFrame(data={'特征': missing.index, '缺失值个数': missing.values})#.index相当于二维数组的下标，把missing由series变为dataframe,dataframe有两列，特征和缺失值个数
    # 通过~取反，选取不包含数字0的行
    missing = missing[~missing['缺失值个数'].isin([0])]#把缺失值个数为0的那些行去掉
    # 缺失比例
    missing['缺失比例'] = missing['缺失值个数'] / X_missing_reg.shape[0]#shape[0]行数，加一个缺失比例列
    print(datapath)
    print(missing)

    X_df = X_missing_reg.isnull().sum()#这不跟missing最初一样吗,是个series,line29
    # 得出列名 缺失值最少的列名 到 缺失值最多的列名
    colname = X_df[~X_df.isin([0])].sort_values().index.values
    # 缺失值从小到大的特征顺序
    sortindex = []
    for i in colname:
        sortindex.append(X_missing_reg.columns.tolist().index(str(i)))#list.index()返回特定元素位于列表中的索引。因为我们得到的column_name是按缺失值数目的多少排列的，而不是原本数据中列的顺序
    # 遍历所有的特征，从缺失最少的开始进行填补，每完成一次回归预测，就将预测值放到原本的特征矩阵中，再继续填补下一个特征
    for i in sortindex:
        # 构建我们的新特征矩阵和新标签
        df = X_missing_reg  # 是原始数据+temp
        fillc = df.iloc[:, i]  # 缺失值最少的特征列的所有数据，
        # 除了第 i 特征列，剩下的特征列+原有的完整标签 = 新的特征矩阵
        df = pd.concat([df.drop(df.columns[i], axis=1), pd.DataFrame(y_full)], axis=1)#axis：用于确定要删除的是行还是列，0表示行，1表示列。y_full就是我们最开始构建的temp那列
        # 在新特征矩阵中，对含有缺失值的列，进行0的填补 ，每循环一次，用0填充的列越来越少
        df_0 = SimpleImputer(missing_values=np.nan, strategy='constant', fill_value=0).fit_transform(df)#SimpleImputer用0填补df（除去i那行）中的缺失值，
        # 找出训练集和测试集
        # 标签
        Ytrain = fillc[fillc.notnull()]  # 该列中没有缺失的部分，就是 Y_train
        Ytest = fillc[fillc.isnull()]  # 不是需要Ytest的值，而是Ytest的索引
        # 特征矩阵
        Xtrain = df_0[Ytrain.index, :]#没有缺失的部分对应的行（一整行哦，不过要除去要预测的第i个列在该行的值）
        Xtest = df_0[Ytest.index, :]  # 有缺失值的特征情况
        rfc = RandomForestRegressor(n_estimators=100)  # n_estimators默认=100，森林中的树数量。
        rfc = rfc.fit(Xtrain, Ytrain)  # 用标签和特征矩阵训练
        Ypredict = rfc.predict(Xtest)  # 用特征矩阵预测标签，就是要填补缺失值的值
        # 将填补好的特征返回到我们的原始的特征矩阵中
        X_missing_reg.loc[X_missing_reg.iloc[:, i].isnull(), X_missing_reg.columns[i]] = Ypredict#应该是series之间的赋值

    # 导出完整的数据
    X_missing_reg.to_csv(outpath)


if __name__ == '__main__':
    #构建路径
    datapath=[r'E:\nuaa\database\NCAA2022\4.csv',r'E:\nuaa\database\NCAA2022\8.csv',r'E:\nuaa\database\NCAA2022\12.csv',r'E:\nuaa\database\NCAA2022\16.csv']
    outpath=[r'E:\nuaa\database\NCAA2022\A.csv',r'E:\nuaa\database\NCAA2022\B.csv',r'E:\nuaa\database\NCAA2022\C.csv',r'E:\nuaa\database\NCAA2022\D.csv']
    for data,out in zip(datapath,outpath):
        DataCompletion(data, out)
    print("数据补全完成")