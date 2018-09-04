import pyhdfs

class file():
    def __init__(self,hosts = '39.104.186.210',port = '9000',user_name = 'spark'):
        self.fs = pyhdfs.HdfsClient(hosts,port,user_name)

    def getFiles(self,path,owner,group):
        try:
            data = []
            for x in self.fs.list_status(path):
                if x['owner'] == owner and x['group'] == group:
                    while path.endswith('/'):
                        path = path[0:len(path)-1]
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

    def getFiletree(self):
        try:
            return self.filetree('/')
        except Exception as e:
            print(e)

    def filetree(self,path):
        files=[]
        if path == '/':
           files.append(path)
        else:
            files = path.split('/')
        list = []
        for x in self.fs.list_status(path):
            if x['type'] == 'DIRECTORY':
                if path.endswith('/'):
                    list.append(self.filetree(path+x['pathSuffix']))
                else :
                    list.append(self.filetree(path +'/'+ x['pathSuffix']))
            else:
                target = {'pathSuffix':x['pathSuffix'],'type':x['type']}
                list.append(target)

        return  {files[len(files)-1]:list}




if __name__ == '__main__':
    file = file();
    print(file.getFiletree())

