# founderOrgcubeBigData
fsdfsd

一.事先准备

1.安装Git

Git下载: http://git-scm.com/downloads 最新版本是2.1.2

2.注册GitHub账号

3.设置代理(可选)

GitHub有一定记录是被墙的…在公司里能用Chrome直接改hosts访问，但是用Idea就无法连接上。所以有必要使用代理、

GoAgent自己去搜一下下载啦

开启GoAgent代理之后，默认是127.0.0.1:8087 作为VPN的地址。

 

二.IntelliJ Idea添加Git与GitHub支持

Idea本身就支持Git和GitHub，不过先要设置Git的位置和跟你的GitHub账号连接起来。

1.设置Git的路径

在Idea中，File-Settings-Version Control-Git 中，在右侧指定git.exe的位置

2.添加GitHub账号

2-0.在Idea中设置代理(可选)

Settings-HTTP Proxy中，在右侧选中Manual proxy configuration

host name 填127.0.0.1

Port number 填8087
2-1.设置GitHub账号

Settings-Version Control-GitHub

host填写github.com

Login和password 分别填写自己的GitHub账号密码

然后下面的Connection timeout 连接时长上限
修改成一个比较大的值
比如50000(输入完记得回车，不然设置没有生效)

然后尝试点Test

如果弹框如下，则说明连接成功。

3、项目的本地git提交
intellij内部集成了git版本控制 所以在本地可以直接进行使用
3.1创建本地仓库
3.2提交代码到本地git
4.配置远程提交
4.1 github上创建仓库
4.2 Git Shell中配置远程仓库
cd 项目目录
右键项目或者文件 Git——Add——Commit （先add 然后再提交）
#更新操作(相当于从github上下载代码)
git pull origin master
#上传操作
git push -u origin master
