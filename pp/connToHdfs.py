from hdfs import TokenClient

client = TokenClient('http://10.91.228.145:50070', 'hdfs',root='/')
print client.list('/user/hdfs/test')
print client.content('/user/hdfs/test')
#client.download(
client.download('/user/hdfs/aichuche/t_reportData101/20140909/1234.txt','f:\\12345.txt')
