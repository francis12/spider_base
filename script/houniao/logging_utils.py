import time, os


class Logger:

    @staticmethod
    def log(log_info=None, this_dir = 'log'):
        if log_info is None:
            return False
        try:
            str(log_info)
        except:
            return False
        this_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        file_name = time.strftime('%Y-%m-%d', time.localtime()) + '.log'
        if os.path.exists('./' + this_dir) is False:
            os.mkdir('./' + this_dir)
        with open('./' + this_dir + '/' + file_name, 'a+') as f:
            f.write('[' + this_time + ']:' + str(log_info) + '\n')
