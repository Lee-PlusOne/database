#有无索引查询
#在query的基础上，在向btree树插入数据的同时加上其id，但查找不按id来，id只是为了返回其所在行数
import pandas as pd
class BTreeNode:
    def __init__(self, leaf=False):
        self.leaf = leaf
        self.keys = []
        self.child = []


class BTree:
    def __init__(self, t):
        self.root = BTreeNode(True)
        self.t = t

    def insert(self, k):
        root = self.root
        if len(root.keys) == (2 * self.t) - 1:
            temp = BTreeNode()
            self.root = temp
            temp.child.insert(0, root)
            self.split_child(temp, 0)
            self.insert_non_full(temp, k)
        else:
            self.insert_non_full(root, k)

    def insert_non_full(self, x, k):
        i = len(x.keys) - 1
        if x.leaf:
            x.keys.append((None, None))
            while i >= 0 and k[1] < x.keys[i][1]:
                x.keys[i + 1] = x.keys[i]
                i -= 1
            x.keys[i + 1] = k
        else:
            while i >= 0 and k[1] < x.keys[i][1]:
                i -= 1
            i += 1
            if len(x.child[i].keys) == (2 * self.t) - 1:
                self.split_child(x, i)
                if k[1] > x.keys[i][1]:
                    i += 1
            self.insert_non_full(x.child[i], k)

    def split_child(self, x, i):
        t = self.t
        y = x.child[i]
        z = BTreeNode(y.leaf)
        x.child.insert(i + 1, z)
        x.keys.insert(i, y.keys[t - 1])
        z.keys = y.keys[t: (2 * t) - 1]
        y.keys = y.keys[0: t - 1]
        if not y.leaf:
            z.child = y.child[t: 2 * t]
            y.child = y.child[0: t - 1]

    def print_tree(self, x, l=0):
        print("Level ", l, " ", len(x.keys), end=":")
        for i in x.keys:
           # print("(",i[0],'{:.10}'.format(i[1]), end=") ")
           print(i, end=" ")
        print()
        l += 1
        if len(x.child) > 0:
            for i in x.child:
                self.print_tree(i, l)

    def index_search(self, data):
        return self._search(self.root, data)

    def _search(self, x, data):
        count=0
        i = 0

        while i < len(x.keys)  and data > x.keys[i][1]:
            i += 1
            count += 1
        one = x.keys[i][1]
         # 查找次数
        two = x.keys[i][1]
        if i < len(x.keys) and data == x.keys[i][1]:
            print("索引查找：已找到", x.keys[i][1], ",查找次数：", count, "id为", x.keys[i][0])
            return True
        elif x.leaf:
            return False
        else:
            return self._search(x.child[i], data)



if __name__ == '__main__':

    data = pd.read_csv(r'E:\nuaa\database\NCAA2022\A.csv')
    # 输入要查找的数据select from  table where x=
    query = input("select * from table 4 where i=? ")
    query=float(query)

    #无索引查询
    count=0
    for j in range(0,len(data)-1):
        if data.at[j,'X']==query:
            print("无索引查找：已找到",data.at[j,'X'] , ",查找次数：", count, "id为", data.at[j,'ID'])
            break;
        else:
            count+=1

    #有索引查询
    column=data[['ID','X']]
    tuples = [tuple(x) for x in column.values]
    B = BTree(20)
    for i in tuples:
        B.insert(i)
   # B.print_tree(B.root)
    ret=B.index_search(query)
    if ret==False:
        print("索引查询：未找到符合条件的数据")
