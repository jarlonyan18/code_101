#列出来当前pod
kubectl get pods | grep sort
kubectl get pods -n tf_model_dump


#进入某个pod
kubectl exec -it xxxx(pod_name) -- bash

#看配置
kubectl describe pod xxxx(pod_name)

#删除某个pod
kubectl delete pod xxx(pod_name)
kubectl get pod | grep key_word | awk '{print $1}' | xargs kubectl delete pod  #批量删除

#修改pod配置，会重启，修改force-update，1改成2或者3都行
kubectl edit deploy xxx(pod_name)
在template:
    metadata:
        annotations:
            force-update: "1"

#查看某个服务的配置,可以看多少cpu核、mem内存
kubectl get deploy | grep sec-sort
kubectl describe deploy data-byteair-sec-sort-online-24c48g-1


#查看一台物理机上部署了哪些实例，先登录上去，然后
kgnp 10.145.64.175
