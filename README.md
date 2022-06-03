# dba in cloud

Python 3.7

一些对腾讯云数据库的操作集合，存在模块复用，有些需要后台一直运行。

start
--------------
### 配置信息修改 ###
```
#第一次使用时需要将配置文件样例复制成配置文件
cp config/config.py.example config/config.py
```

### 运行 ###
```
#原则支持任意路径运行，但安全起见最好先切换到项目的根目录

python3 bin/xxx.py
python3 test/xxx.py
```

