
#拉取镜像
docker pull xxxxx


#启动镜像
docker run -itd --name [容器名字] [镜像名字:镜像tag] /bin/bash
-d 会将容器保持后台运行，否则执行/bin/bash的话会立即执行完毕导致容器退出

#查看容器列表
docker ps -a

#进入一个容器，开始交互
docker exec -it [短ID] /bin/bash

