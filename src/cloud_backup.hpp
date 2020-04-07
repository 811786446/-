#include <cstdio>
#include <string>
#include <iostream>
#include <vector>
#include <unordered_map>
#include <fstream>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <zlib.h>
#include <pthread.h>
#include "httplib.h"

#define NONHOT_TIME 30     //最后一次访问时间在30秒以内
#define INTERVAL_TIME 60     //非热点的检测每60秒一次
#define BACKUP_DIR "./backup/"     //文件的备份路径
#define GZFILE_DIR "./gzfile/"     //压缩包存放路径
#define DATA_FILE "./list.backup"          //数据管理模块的数据备份文件名称
namespace _cloud_sys{
    class FileUtil{
        public:
            //从文件中读取所有内容
            static bool Read(const std::string &name, std::string *body){
                std::ifstream fs(name, std::ios::binary); //二进制形式打开输入文件流
                //name默认解析路径名称
                if(fs.is_open() == false){
                    std::cout << "open file " << name << "failed\n";
                    return false;
                }
                //获取文件大小
                int64_t fsize = boost::filesystem::file_size(name);
                body->resize(fsize); //body申请空间
                fs.read(&(*body)[0], fsize);
                //fs.good 判断流状态，即上次操作是否成功 
                if(fs.good() == false) {
                    std::cout << "file " << name << "read data failed!\n";
                    return false;
                }
                fs.close();
                return true;
            }
            //向文件中写入数据
            static bool Write(const std::string &name, const std::string &body){
                //覆盖写入
                std::ofstream ofs(name, std::ios::binary);
                if(ofs.is_open() == false) {
                    std::cout << "open file " << name << "failed!\n";
                    return false;
                }
                ofs.write(&body[0], body.size());
                if(ofs.good() == false){
                    std::cout << "file " << name << "write data failed!\n";
                    return false;
                }
                ofs.close();
                return true;
            }
    };
    class CompressUtil {
        public:
            //文件压缩-源文件名 - 压缩包名
            static bool Compress(const std::string &src, const std::string &dst){
                std::string body;
                FileUtil::Read(src, &body);
                //打开压缩包
                gzFile gf = gzopen(dst.c_str(), "wb");
                if(gf == NULL){
                    std::cout << "open file " << dst << "failed!\n";
                    return false;
                }
                int wlen = 0;
                while(wlen < (int)body.size()){
                    int ret = gzwrite(gf, &body[wlen], body.size() - wlen);
                    if(ret == 0){
                        std::cout << "file " << dst << "write compress data failed!\n ";
                        return false;
                    }
                    wlen += ret;
                }
                gzclose(gf);
                return true;
            }
            //文件解压缩 -压缩包名 - 源文件名
            static bool UnCompress(const std::string &src, const std::string &dst){

                std::ofstream ofs(dst, std::ios::binary);
                if(ofs.is_open() == false){
                    std::cout << "open file " << dst << "failed!\n";
                    return false;
                }
                gzFile gf = gzopen(src.c_str(), "rb");
                if(gf == NULL){
                    std::cout << "open file " << src << "failed!\n";
                    ofs.close();
                    return false;
                }
                int ret = 0;
                char tmp[4096] = {0};
                while((ret = gzread(gf, tmp, 4096)) > 0){
                    ofs.write(tmp ,ret);
                }
                ofs.close();
                gzclose(gf);
                return true;
            }
    };
    class DataManager
    {
        public:
            DataManager(const std::string &path)
                :_back_file(path){
                pthread_rwlock_init(&_rwlock, NULL);
            }
            ~DataManager(){
                pthread_rwlock_destroy(&_rwlock);
            }
            //判断文件是否存在
            bool Exists(const std::string &name){
                //在_file_list中是否能够找到这个文件
                pthread_rwlock_rdlock(&_rwlock);
                auto it = _file_list.find(name);
                if(it == _file_list.end()){
                    //到达结尾说明没有找到
                    pthread_rwlock_unlock(&_rwlock);
                    return false;
                }
                pthread_rwlock_unlock(&_rwlock);
                return true;
            }
            //判断文件是否已经被压缩
            bool IsCompress(const std::string &name){
                //管理的数据：<源文件名称，压缩包名称>
                //文件上传后，源文件名称和压缩包名称相同
                //压缩之后，将压缩包名称更新为指定的包名
                pthread_rwlock_rdlock(&_rwlock);
                auto it = _file_list.find(name);
                if(it == _file_list.end()){
                    pthread_rwlock_unlock(&_rwlock);
                    return false;
                }
                if(it->first == it->second){
                    pthread_rwlock_unlock(&_rwlock);
                    return false;
                }
                pthread_rwlock_unlock(&_rwlock);
                return true;
            }
            //获取未压缩文件列表
            bool NonCompressList(std::vector<std::string> *list){
                //将没有压缩的文件添加到list中
                pthread_rwlock_rdlock(&_rwlock);
                for(auto it = _file_list.begin(); it != _file_list.end(); ++it){
                    if(it->first == it->second){
                        list->push_back(it->first);
                    }
                }
                pthread_rwlock_unlock(&_rwlock);
                return true;
            }
            //根据源文件名获取压缩文件名称
            bool GetGzName(const std::string &src, std::string *dst){
              auto it = _file_list.find(src);
              if(it == _file_list.end()){
                return false;
              }
              *dst = it->second;
              return true;
            }
            //插入 / 更新数据
            bool Insert(const std::string &src, const std::string &dst){
                pthread_rwlock_wrlock(&_rwlock);
                _file_list[src] = dst;
                pthread_rwlock_unlock(&_rwlock);
                Storage();
                return true;
            }
            //获取所有文件名称
            bool GetAllName(std::vector<std::string> *list){
                pthread_rwlock_rdlock(&_rwlock);
                for(auto it = _file_list.begin(); it != _file_list.end(); ++it){
                    list->push_back(it->first);
                } 
                pthread_rwlock_unlock(&_rwlock);
                return true;
            }
            //数据改变后持久化存储
            bool Storage(){
                //将_file_list中的数据进行持久化存储
                //数据对象进行持久化存储 --- 序列化
                //src dst\r\n
                std::stringstream tmp;
                pthread_rwlock_rdlock(&_rwlock);
                for(auto it = _file_list.begin(); it != _file_list.end(); ++it){
                    tmp << it->first << ' ' << it->second << "\r\n";
                }
                pthread_rwlock_unlock(&_rwlock);
                //每次都是清空重新写入
                //tmp.str() 获取string流中的string对象
                FileUtil::Write(_back_file, tmp.str());
                return true;

            }
            //启动时初始化加载原有数据
            bool InitLoad(){
                //从数据的持久化存储中加载数据
                //1. 将这个备份文件的数据读取出来
                 std::string body;
                 if(FileUtil::Read(_back_file, &body) == false){
                     return false;
                 }

                //2. 进行字符串处理，按照\r\n进行分割
               //boost::split(vector, src, sep, flag)
                std::vector<std::string> list;
                boost::split(list, body, boost::is_any_of("\r\n"), 
                        boost::token_compress_off);
                //3. 每一行按照空格进行分割-前边是key.后边是val
                for(auto i : list){
                    size_t pos = i.find(" ");
                    if(pos == std::string::npos){
                        continue;
                    }
                    std::string key = i.substr(0, pos);
                    std::string val = i.substr(pos + 1);
                //4. 将key/val添加到_file_list中
                Insert(key, val);
                }
            }
        private:
            std::string _back_file;    //持久化数据存储文件名
            std::unordered_map<std::string, std::string> _file_list; //数据管理容器
            pthread_rwlock_t _rwlock;   //读写锁，读共享写互斥
    };
    DataManager data_manage(DATA_FILE); //不能定义类前面
    class NonHotFile {
        public:
            NonHotFile(const std::string gz_dir, const std::string bu_dir)
                :_gz_dir(gz_dir)
                ,_bu_dir(bu_dir)
             {}
            //总体向外提供d功能接口，开始压缩文件
            bool Start(){
                //是一个循环的，持续的过程 - 每隔一段时间，判断有没有非热点文件，然后压缩
                while(1){
                    //1. 获取所有的未压缩文件列表
                    std::vector<std::string> list;
                    data_manage.NonCompressList(&list);
                    //2. 逐个判断这个文件是否是热点文件
                    for(int i = 0; i < list.size(); i++){
                        bool ret = FileIsHot(list[i]);
                        if(ret == false){
                            std::string s_filename =list[i];
                            std::string d_filename = list[i] + ".gz";
                            //源文件名
                            std::string src_name = _bu_dir + s_filename;
                            //压缩文件名
                            std::string dst_name = _gz_dir + d_filename + ".gz";
                    //3. 如果是非热点文件，压缩这个文件，删除源文件 
                            if(CompressUtil::Compress(src_name, dst_name) == true){
                                data_manage.Insert(s_filename, d_filename); // 更新数据信息
                                unlink(src_name.c_str()); // 删除源文件
                            }
                        }
                    }
                    //4. 休眠一会
                    sleep(INTERVAL_TIME);
                }
                return true;
            }
        private:
            //判断文件是否是一个热点文件
            bool FileIsHot(const std::string &name){
                time_t cur_t = time(NULL);
                //获取状态结构体stat,里面的st_atime最后一次访问时间
                struct stat st;
                if(stat(name.c_str(), &st) < 0){
                        std::cout << "get file " << name << "stat failed!\n";
                        return false;
                }
                if((cur_t - st.st_atime) > NONHOT_TIME){
                        return false;
                }
                return true;
            }
        private:
            std::string _bu_dir;
            std::string _gz_dir;   //压缩后的文件存储路径
    };

    class Server{
        public:
          //启动网络通信模块接口
          bool Start(){
            _server.Put("/(.*)", FileUpload);
            _server.Get("/list", FileList);
            _server.Get("/download/(.*)", FileDownLoad); //.* 匹配任意字符串
           // 搭建tcp服务器，进行http数据接收处理
           //接收到一个req之后，抛入线程池解析，调用回调函数，进行业务处理，然后填充rsp，返回
            _server.listen("0.0.0.0", 9000);
            return true;
          }
        private:
            //文件上传处理回调函数
          static void FileUpload(const httplib::Request &req, httplib::Response &rsp){
            //req.matches[1] 表示用正则表达式 .* 捕捉到的字符串
            // [0] 表示全部字符串
            std::string filename = req.matches[1];
            std::string pathname = BACKUP_DIR + filename;
            //向文件中写入数据，如果文件不存在，则创建，
            //因为设计的上传文件的请求的正文就是文件数据，所以直接将body写入即可。
            FileUtil::Write(pathname, req.body);
            data_manage.Insert(filename, filename);
            rsp.status = 200;
            return;
          }
            //文件列表处理函数
          static void FileList(const httplib::Request &req, httplib::Response &rsp){
            std::vector<std::string> list;
            data_manage.GetAllName(&list);
            std::stringstream tmp;
            tmp << "<html><body><hr />";
            for(int i = 0; i < list.size(); i++){
              //超链接
              //用户点击这个连接之后，向服务器发送一个herf后面的连接请求(Get)
              tmp << "<a herf='/download/" << list[i] <<"'>" << list[i] << "</a>";
              tmp << "<hr /";
            }
            tmp << "<hr /></body></html>";
            rsp.set_content(tmp.str().c_str(), tmp.str().size(), "text/html");
            rsp.status = 200;
            return ;
          }
            //文件下载处理函数
          static void FileDownLoad(const httplib::Request &req, httplib::Response &rsp){

            std::string filename = req.matches[1];
            if(data_manage.Exists(filename) == false){
              rsp.status = 404; 
              return ;
            }
            //源文件的备份路径名
            std::string pathname = BACKUP_DIR + filename;
            if(data_manage.IsCompress(filename) != false){
              //文件被压缩，先解压出来
              std::string gzfile;
              //将压缩包路径+文件名组织起来
              data_manage.GetGzName(filename, &gzfile);
              std::string gzpathname = GZFILE_DIR + gzfile;
              //解压
              CompressUtil::UnCompress(gzpathname, pathname);
              //删除压缩包
              unlink(gzpathname.c_str());
              //更新数据
              data_manage.Insert(filename,filename);
            }

              //如果文件未被压缩，则直接读取数据，直接将文件数据读取到rsp.body中
              FileUtil::Read(pathname, &rsp.body);
              //application/octet-stream 表示二进制流下载
              rsp.set_header("Content-Type", "application/octet-stream");
              rsp.status = 200;
              return;
          }
        private:
            std::string _file_dir;    //文件上传    备份路径
            httplib::Server _server;
    };
}

