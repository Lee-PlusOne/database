import pandas as pd
import time
import os
os.chdir(r'E:\nuaa\database\NCAA2022')
data = pd.read_csv('A.csv', encoding='gbk')
#无索引查询
def query(cmd:list):#二维list
    X1 = cmd[0][0]
    X2 = cmd[0][1]
    Y1 = cmd[1][0]
    Y2 = cmd[1][1]
    Z1 = cmd[2][0]
    Z2 = cmd[2][1]
    W1 = cmd[3][0]
    W2 = cmd[3][1]
    start = time.time()#获取当前时间
    for i in range(0, len(data) - 1):
        if (data.iloc[i, 2] >= float(X1)) and (data.iloc[i, 2] <= float(X2)):#loc[i]默认读取第i+1行的值，iloc[i]默认读取第i+1列的值，而iloc[i;1]读取第i+1行，第2列的值
            if (data.iloc[i, 3] >= float(Y1)) and (data.iloc[i, 3] <= float(Y2)):
                if (data.iloc[i, 4] >= float(Z1)) and (data.iloc[i, 4] <= float(Z2)):
                    if (data.iloc[i, 5] >= float(W1)) and (data.iloc[i, 5] <= float(W2)):
                        list = {data.iloc[i, 2], data.iloc[i, 3], data.iloc[i, 4], data.iloc[i, 5]}
                        print(list)
    end = time.time()
    print("查询时间为", end - start)

if __name__ == '__main__':
    while True:
        print("请以 [[X1,X2],[Y1,Y2],[Z1,Z2],[W1,W2]]的格式输入查询窗口:")
        cmd = input()
        import sys
        if cmd == 'exit;':
            sys.exit()
        query(eval(cmd))