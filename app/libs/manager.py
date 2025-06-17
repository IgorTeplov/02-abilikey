from libs.dirs import (
    PIPE_DIR, LOG_DIR, REQUEST_DIR, EXECUTION_LOG_DIR, REQUEST_LOG_DIR
)
from json import loads
from datetime import datetime, timedelta


class Manager:

    def __init__(self, log_conf, pipe_conf, requests_conf):
        self.log_conf = log_conf
        self.pipe_conf = pipe_conf
        self.requests_conf = requests_conf

        self.PIPE_DIR = self.pipe_conf["pipeline_dir"]
        self.REQUEST_DIR = self.requests_conf["requests_dir"]
        self.LOG_DIR = self.log_conf["log_dir"]
        self.REQUEST_LOG_DIR = self.log_conf["request_log_dir"]
        self.EXECUTION_LOG_DIR = self.log_conf["execution_log_dir"]

    def ask(self, question):
        answer = input(f"{question}? [Y/N]:")
        if answer in ["Yes", "yes", "Y", "y"]:
            return True
        return False

    @classmethod
    def rmdir(cls, directory):
        for item in directory.iterdir():
            if item.is_dir():
                cls.rmdir(item)
            else:
                item.unlink()
        directory.rmdir()

    def print_exec(self, id_, exec_):
        print(f"START: {exec_['start']}")
        print(f"ID: {id_}")
        print(f"DURATION: {exec_['duration']}")
        finished = "YES" if exec_['finished'] else "NO"
        print(f"NAME: {exec_['name']}")
        print(f"STAGE: {exec_['last_step']}")
        print(f"FINISHED: {finished}")
        print()

    def get_execs(self, show=False):
        execs = []
        for exec_ in self.PIPE_DIR.glob("*/"):
            data = loads((exec_ / "status.json").read_text())
            data["start"] = datetime.fromisoformat(data["start"])
            execs.append(data)
        execs.sort(key=lambda item: item['start'])
        execs = {item["id"]: item for item in execs}

        if show:
            range_ = slice(0, 10)
            if len(list(execs.items())) > 10:
                if self.ask("We have more than 10 records, show all"):
                    range_ = slice(0, len(list(execs.items())) + 1)

            for id_, exec_ in list(execs.items())[range_]:
                self.print_exec(id_, exec_)
        return execs

    def get_exec(self, id_):
        execs = self.get_execs()
        if id_ in execs:
            exec_ = execs[id_]
            self.print_exec(id_, exec_)
            return exec_
        else:
            print("Sorry but we don't have any record with this ID.")

    def clear_old_record(self, hours):
        execs = self.get_execs()
        now = datetime.now()
        for id_, exec_ in execs.items():
            if now - exec_['start'] >= timedelta(hours=hours):
                d = self.EXECUTION_LOG_DIR / id_
                if d.is_dir():
                    self.rmdir(d)
                d = self.REQUEST_LOG_DIR / id_
                if d.is_dir():
                    self.rmdir(d)
                d = self.REQUEST_DIR / id_
                if d.is_dir():
                    self.rmdir(d)
                d = self.PIPE_DIR / id_
                if d.is_dir():
                    self.rmdir(d)

    def show_active(self):
        execs = self.get_execs()
        records = list(execs.values())
        records = filter(lambda obj: not obj['finished'], records)
        for exec_ in records:
            id_ = exec_['id']
            self.print_exec(id_, exec_)

    def show_today(self):
        execs = self.get_execs()
        records = list(execs.values())
        records = filter(lambda obj: datetime.now() - obj['start'] <= timedelta(hours=24), records)
        for exec_ in records:
            id_ = exec_['id']
            self.print_exec(id_, exec_)


manager = Manager({
    "log_dir": LOG_DIR,
    "request_log_dir": REQUEST_LOG_DIR,
    "execution_log_dir": EXECUTION_LOG_DIR
}, {
    "pipeline_dir": PIPE_DIR
}, {
    "requests_dir": REQUEST_DIR
})
