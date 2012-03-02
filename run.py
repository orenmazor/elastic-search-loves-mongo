from oplog import Oplog

def run():
	op_emitter = Oplog()
	op_emitter.start()

if __name__ == "__main__":
	run()