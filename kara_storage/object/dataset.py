import os
from typing import Any, List
import threading
import queue
import hashlib
from ..abc import StorageBase
from tqdm import tqdm

class ObjectDataset:
    def __init__(self, storage : StorageBase, num_workers=4, chunk_size = 128 * 1024):
        self.__storage = storage
        self.num_workers = num_workers
        self.chunk_size = chunk_size
    
    def __calc_local_file_hash(self, path):

        md5 = hashlib.md5()
        sha1 = hashlib.sha1()
        
        fp = open(path, "rb")
        while True:
            data = fp.read(self.chunk_size)
            if len(data) == 0:
                break
            md5.update(data)
            sha1.update(data)
        fp.close()
        return md5.hexdigest(), sha1.hexdigest()


    def __download_thread(self, q : queue.Queue, path_prefix, remote_prefix, q_bar : queue.Queue):
        while True:
            try:
                v = q.get_nowait()
            except queue.Empty:
                break
            local_path = os.path.join(path_prefix, *v["path"])
            if os.path.exists(local_path):
                # local file exists, check hash
                md5, sha1 = self.__calc_local_file_hash(local_path)
                if v["md5"] == md5 and v["sha1"] == sha1:
                    # same file, ignore
                    q_bar.put( v["size"] )
                    continue
            
            # download remote file
            f_remote = self.__storage.open(remote_prefix + v["file"], "r")

            # makedir for local file
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            f_local = open(local_path, "wb")
            
            # trunk downloader
            memview = bytearray(self.chunk_size)
            while True:
                lw = f_remote.readinto(memview)
                if lw == 0 or lw is None:
                    break
                q_bar.put(lw)
                f_local.write( memview[:lw] )
            f_remote.close()
            f_local.close()

    def download(self, data_prefix : str, file_info, local_path : str, progress_bar=True) -> None:
        # normalize data_prefix
        if not data_prefix.endswith("/"):
            data_prefix = data_prefix + "/"
        
        q_file = queue.Queue()
        total_file_size = 0
        for it in file_info:
            q_file.put(it)
            total_file_size += it["size"]
        
        q_bar = queue.Queue()
        thds = [
            threading.Thread(target=self.__download_thread, args=(q_file, local_path, data_prefix, q_bar), daemon=True) 
                for _ in range(self.num_workers)
        ]
        thd_bar = threading.Thread(target=self.__progress_bar, args=(q_bar, progress_bar), kwargs={"total": total_file_size, "desc": "Downloading", "unit": "B", "unit_scale": True}, daemon=True)
        thd_bar.start()
        for thd in thds:
            thd.start()
        
        for thd in thds:
            thd.join()
        q_bar.put(None)
        thd_bar.join()
        
        path_set = set()
        for it in file_info:
            str_path = "/".join(it["path"])
            path_set.add(str_path)
        
        for fullname, relative_path in self.__search_in_file(local_path, []):
            str_path = "/".join(relative_path)
            if str_path not in path_set:
                os.unlink(fullname)

    def __progress_bar(self, q_upd : queue.Queue, progress_bar : bool, *args, **kwargs):
        if progress_bar:
            with tqdm(*args, **kwargs) as pbar:
                while True:
                    v = q_upd.get()
                    if v is None:
                        break
                    pbar.update(v)
        else:
            while True:
                v = q_upd.get()
                if v is None:
                    break

    
    def __search_in_file(self, local_path, path : List[str]):
        for fname in os.listdir(local_path):
            
            fullname = os.path.join(local_path, fname)
            relative_path = (path + [fname]).copy()

            if os.path.isdir(fullname):
                for it in self.__search_in_file(fullname, relative_path):
                    yield it
            else:
                yield (fullname, relative_path)
    
    def __hash_thread(self, q_in : queue.Queue, q_out : queue.Queue, q_bar : queue.Queue):
        while True:
            try:
                local_path, relative_path = q_in.get_nowait()
            except queue.Empty:
                break
            md5, sha1 = self.__calc_local_file_hash(local_path)
            q_out.put({
                "md5": md5,
                "sha1": sha1,
                "file": md5 + "_" + sha1,
                "path": relative_path,
                "size": os.stat(local_path).st_size
            })
            q_bar.put(1)
    
    def __upload_thread(self, q_in : queue.Queue, remote_path : str, q_bar : queue.Queue):
        while True:
            try:
                info = q_in.get_nowait()
            except queue.Empty:
                break
            
            if self.__storage.filesize(remote_path + info["file"]) is None:
                # file not exists
                self.__storage.put( remote_path + info["file"] , open(info["local_path"], "rb") )
            q_bar.put( info["size"] )
            
                
    def upload(self, data_prefix : str, local_path : str, progress_bar=True) -> Any:
        # normalize data_prefix
        if not data_prefix.endswith("/"):
            data_prefix = data_prefix + "/"
        
        q_in = queue.Queue()
        q_out = queue.Queue()
        q_bar = queue.Queue()

        cnt_files = 0
        for it in self.__search_in_file(local_path, []):
            q_in.put(it)
            cnt_files += 1

        # start hash thread
        thds = [
            threading.Thread(target=self.__hash_thread, args=(q_in, q_out, q_bar), daemon=True) 
                for _ in range(self.num_workers)
        ]
        thd_bar = threading.Thread(target=self.__progress_bar, args=(q_bar, progress_bar), kwargs={"total": cnt_files, "desc": "Hashing"}, daemon=True)
        thd_bar.start()
        for thd in thds:
            thd.start()

        for thd in thds:
            thd.join()
        q_bar.put(None)
        thd_bar.join()

        # end hash threads
        file_info = []
        while not q_out.empty():
            file_info.append( q_out.get() )
        
        assert q_in.empty()
        assert q_out.empty()
        assert q_bar.empty()

        # filter identical files

        total_file_size = 0
        file_content_set = set()
        for info in file_info:
            if info["file"] not in file_content_set:
                file_content_set.add( info["file"] )
                fpath = os.path.join(local_path, *info["path"])
                total_file_size += os.stat(fpath).st_size
                q_in.put({
                    "file": info["file"],
                    "local_path": fpath,
                    "size": info["size"]
                })
        
        # start upload threads
        thds = [
            threading.Thread(target=self.__upload_thread, args=(q_in, data_prefix, q_bar), daemon=True)
                for _ in range(self.num_workers)
        ]
        
        thd_bar = threading.Thread(target=self.__progress_bar, args=(q_bar, progress_bar), kwargs={"total": total_file_size, "desc": "Uploading", "unit": "B", "unit_scale": True}, daemon=True)
        thd_bar.start()
        for thd in thds:
            thd.start()
        
        for thd in thds:
            thd.join()
        q_bar.put(None)
        thd_bar.join()
        return file_info
        
        
        
        

