import configparser
import os
import platform



class XKCCommonParam():
    base_dir = os.path.dirname(__file__)
    config_file_path = os.path.join(base_dir, "xkucun_common.ini")  # 配置文件名称
    config = configparser.ConfigParser()
    config.read(config_file_path,encoding="utf-8-sig")

    # 数据库链接
    database_url = config["database"]["url"]
    database_pool_size = int(config["database"]["pool_size"])
    database_max_overflow = int(config["database"]["max_overflow"])
    job_hour = config["job"]["hour"]
    job_min = config["job"]["min"]
    # 下载根目录
    plat_form = platform.platform()
    if "Linux" in plat_form:
        script_download_path = config["download.path"]["linux_path"]
    elif "Windows" in plat_form:
        script_download_path = config["download.path"]["win_path"]
    else:
        script_download_path = config["download.path"]["linux_path"]

