import pyhdfs

class file():
    def __init__(self,hosts = '39.104.186.210',port = '9000',user_name = 'spark'):
        self.fs = pyhdfs.HdfsClient(hosts,port,user_name)

    def getFiles(self,path,owner,group):
        try:
            data = []
            for x in self.fs.list_status(path):
                if x['owner'] == owner and x['group'] == group:
                    x['path'] = path +'/'+x['pathSuffix']
                    data.append(x)
            return data
        except Exception as e:
            print(e)

    def deleteFiles(self, path):
        try:
            self.fs.delete(path)
            status = {"status":"操作成功!","code":"200"}
            return  status
        except Exception as e:
            print(e)
            status = {"status":"操作失败!","code":"500"}

if __name__ == '__main__':
    file = file();
    print(file.getFiles("/","spark","supergroup"))

