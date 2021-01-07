#タスクの同時実行と優先順位実行
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import luigi
import pathlib
from luigi.util import inherits,requires

# プロセスC:"Hello luigi"登録
class TaskC(luigi.Task):
    def output(self):
        return luigi.LocalTarget("job3.txt")
    def run(self):
        out = self.output()
        with out.open('w') as f:
            f.write("TaskC")
# プロセスB:"Hello world"登録
class TaskB(luigi.Task):
    def output(self):
        return luigi.LocalTarget("job2.txt")
    def run(self):
        out = self.output()
        with out.open('w') as f:
            f.write("TaskB")
# プロセスA: main実行
class TaskA(luigi.Task):
    def requires(self):
        pass
    def output(self):
        return luigi.LocalTarget("job1.txt")
    def run(self):
        out = self.output()
        with out.open('w') as f:
            f.write("TaskA")
        #taskb = TaskB()
        #taskc = TaskC()
        #yield taskb
        #yield taskc # プロセスAとBが終わったら実行
        #shutil.copy("./job2.txt","./result.txt")
# 依存タスクを並行して実行
class MyInvokerTask(luigi.WrapperTask):
    def requires(self):
        return [TaskA(),TaskB(),TaskC()]
# エントリポイント
def main():
    luigi.run(main_task_cls=MyInvokerTask,local_scheduler=True)
    #luigi.run(main_task_cls=MyInvokerTask,central_scheduler=True)
if __name__ == '__main__':
    main()
