from oplog import Oplog
from multiprocessing import Process

def run():
    op_emitter = Oplog()
    Process(target=op_emitter.start).start()

if __name__ == "__main__":
    run()